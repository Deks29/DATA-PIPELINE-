"""Stage 2 transformation engine for the SightSync batch pipeline.

The engine reads the validated Stage 1 Parquet handover, applies the rules
documented in ``docs/transformation.md``, validates the result (TV-01..TV-14),
writes the ``analytics`` dimensional model as Parquet, and records an audit row
per output table.

Module map
----------
- ``cleaning``     cleaning rules C-01..C-12
- ``validation``   data-quality checks TV-01..TV-14
- ``dimensions``   analytics dimension tables
- ``facts``        analytics fact tables
- ``handover``     acceptance decision, output writing and manifest
- ``metadata``     audit metadata, logs and retention
"""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl

from transformation.cleaning import CleaningMixin
from transformation.constants import (
    APPROVED,
    APPROVED_WITH_WARNING,
    CRITICAL,
    DATE_PARTITION,
    FAIL,
    MANUAL_REVIEW,
    NON_CRITICAL,
    OUTPUT_COLUMNS,
    PASS,
    PRIMARY_KEYS,
    REJECTED,
    REQUIRED_COLUMNS,
    SKIP,
    UNKNOWN_CODE,
    UNKNOWN_USER,
    WARNING,
    iso,
)
from transformation.dimensions import DimensionMixin
from transformation.facts import FactMixin
from transformation.handover import HandoverMixin
from transformation.metadata import TransformMetadataStore, TransformationMetadata, utcnow
from transformation.models import CheckResult, SourceState, TransformationResult
from transformation.validation import ValidationMixin
from transformation.config import TransformSettings, load_transform_settings


class TransformationPipeline(
    CleaningMixin,
    DimensionMixin,
    FactMixin,
    ValidationMixin,
    HandoverMixin,
):
    """Orchestrates the whole Stage 2 transformation for one business date."""

    def __init__(
        self,
        settings: TransformSettings | None = None,
        store: TransformMetadataStore | None = None,
    ) -> None:
        self.settings = settings or load_transform_settings()
        self.store = store or TransformMetadataStore(
            self.settings.metadata_db, self.settings.logs_dir, self.settings.quarantine_dir
        )

    # ------------------------------------------------------------------ public

    def run(self, business_date: str | date | None = None, force: bool = False) -> list[TransformationResult]:
        bdate = self._resolve_business_date(business_date)
        run_id = f"TRF_{bdate:%Y%m%d}_{utcnow():%H%M%S}_{uuid.uuid4().hex[:6]}"
        batch_id = f"{self.settings.pipeline_name}:{bdate.isoformat()}"
        started = utcnow()
        run_at = started

        sources = self._load_sources(bdate)
        transformed, states, zero_qty_input = self._build_all(
            sources, bdate, run_id, batch_id, run_at
        )
        quarantine_counts = self._write_quarantine(states, bdate, run_id, batch_id)

        checks = self._validate(states, transformed, zero_qty_input, run_at)
        decision = self._decide(states, checks)

        output_paths: dict[str, Path] = {}
        if decision != REJECTED:
            output_paths = self._write_outputs(
                transformed, bdate, force=force, batch_id=batch_id
            )

        ended = utcnow()
        duration_ms = int((ended - started).total_seconds() * 1000)
        parent_run_id = self._parent_run_id(states)
        results: list[TransformationResult] = []
        for table_key, frame in transformed.items():
            state = self._state_for_output(table_key, states)
            result = TransformationResult(
                table_name=f"analytics.{table_key}",
                run_id=run_id,
                decision=decision,
                status=self._status_for(decision),
                checks=checks,
                records_read=state.records_read,
                records_output=frame.height,
                records_rejected=state.records_rejected,
                records_deduplicated=state.records_deduplicated,
                records_excluded=state.records_excluded,
                records_modified=state.records_modified,
                rules_applied=dict(state.rules_applied),
                output_path=str(output_paths[table_key]) if table_key in output_paths else None,
                quarantine_path=str(quarantine_counts.get(state.name)) if state.name in quarantine_counts else None,
                message="" if decision != REJECTED else "Transformation rejected; output not published.",
            )
            results.append(result)
            self.store.record(
                TransformationMetadata(
                    run_id=run_id,
                    parent_run_id=parent_run_id,
                    batch_id=batch_id,
                    batch_window_start=bdate.isoformat(),
                    batch_window_end=bdate.isoformat(),
                    table_name=result.table_name,
                    transformation_started_at=iso(started),
                    transformation_ended_at=iso(ended),
                    duration_ms=duration_ms,
                    status=result.status,
                    decision=decision,
                    records_read=result.records_read,
                    records_output=result.records_output,
                    records_rejected=result.records_rejected,
                    records_deduplicated=result.records_deduplicated,
                    records_excluded=result.records_excluded,
                    records_modified=result.records_modified,
                    rules_applied=result.rules_applied,
                    validation_results=result.check_map,
                    transformation_version=self.settings.transformation_version,
                )
            )
        if decision != REJECTED:
            self._write_manifest(
                results, states, bdate, batch_id, parent_run_id, started, ended, duration_ms
            )
        return results

    # --------------------------------------------------------------- source I/O

    def _partitions(self, table: str) -> list[tuple[date, Path]]:
        table_dir = Path(self.settings.staging_dir) / table
        if not table_dir.exists():
            return []
        found: list[tuple[date, Path]] = []
        for path in table_dir.glob("business_date=*"):
            match = DATE_PARTITION.search(path.name)
            if match and (path / "data.parquet").is_file():
                found.append((date.fromisoformat(match.group(1)), path / "data.parquet"))
        return sorted(found)

    def _read_window(
        self,
        table: str,
        start: date | None = None,
        end: date | None = None,
        latest: bool = False,
    ) -> pl.DataFrame:
        partitions = self._partitions(table)
        if latest:
            selected = partitions[-1:]
        elif start is not None or end is not None:
            selected = [
                (d, p)
                for d, p in partitions
                if (start is None or d >= start) and (end is None or d <= end)
            ]
        else:
            selected = partitions
        if not selected and self.settings.allow_latest_fallback and partitions:
            selected = partitions[-1:]
        if not selected:
            return pl.DataFrame()
        frames = [
            pl.read_parquet(path).with_columns(
                pl.lit(d.isoformat()).alias("_source_business_date")
            )
            for d, path in selected
        ]
        return pl.concat(frames, how="diagonal_relaxed")

    def _load_sources(self, bdate: date) -> dict[str, pl.DataFrame]:
        lookback_start = bdate - timedelta(days=self.settings.lookback_days)
        month_start = bdate.replace(day=1)
        previous_month = (month_start - timedelta(days=1)).replace(day=1)
        return {
            "raw_logs": self._read_window("raw_logs", lookback_start, bdate),
            "raw_entities": self._read_window("raw_entities", lookback_start, bdate),
            "hb_pitc": self._read_window("hb_pitc", previous_month, month_start.replace(day=28)),
            "device_classification": self._read_window("device_classification", latest=True),
        }

    # ------------------------------------------------------------ orchestration

    def _build_all(
        self,
        sources: dict[str, pl.DataFrame],
        bdate: date,
        run_id: str,
        batch_id: str,
        run_at: datetime,
    ) -> tuple[dict[str, pl.DataFrame], dict[str, SourceState], int]:
        states = {
            name: SourceState(name=name)
            for name in ("raw_logs", "raw_entities", "hb_pitc", "device_classification")
        }
        self._hb_processed = None
        logs = self._clean_logs(sources["raw_logs"], states["raw_logs"], run_id, batch_id, bdate)
        entities = self._clean_entities(sources["raw_entities"], states["raw_entities"], run_id, batch_id, bdate)
        hb, zero_qty = self._clean_hb(sources["hb_pitc"], states["hb_pitc"], run_id, batch_id, bdate)
        self._hb_processed = hb
        device = self._clean_device(
            sources["device_classification"], states["device_classification"], run_id, batch_id, bdate
        )

        dates: list[date] = []
        if logs.height:
            dates.extend(logs["event_date"].to_list())
        if entities.height:
            dates.extend(entities["created_date"].to_list())
        if hb.height:
            dates.extend(hb["paid_month"].to_list())
        if device.height:
            dates.extend(device["last_updated_date"].to_list())

        dim_date = self._build_dim_date(dates, bdate)
        dim_user = self._build_dim_user(logs, entities, run_at)
        dim_health_board = self._build_dim_health_board(hb, run_at)
        dim_drug = self._build_dim_drug(hb, run_at)
        dim_prescribed_type = self._build_dim_prescribed_type(hb)
        dim_device = self._build_dim_device(device, run_at)

        fact_prescribing = self._build_fact_prescribing(
            hb, dim_date, dim_health_board, dim_drug, dim_prescribed_type, run_id, run_at
        )
        fact_event = self._build_fact_event(logs, dim_date, dim_user, run_id)
        fact_daily = self._build_fact_daily_summary(
            logs, entities, fact_event, dim_date, run_id
        )

        transformed = {
            "dim_date": dim_date,
            "dim_user": dim_user,
            "dim_health_board": dim_health_board,
            "dim_drug": dim_drug,
            "dim_prescribed_type": dim_prescribed_type,
            "dim_device_classification": dim_device,
            "fact_prescribing_monthly": fact_prescribing,
            "fact_event_daily": fact_event,
            "fact_daily_summary": fact_daily,
        }
        return transformed, states, zero_qty

    # ------------------------------------------------------------------- helpers

    def _state_for_output(self, table_key: str, states: dict[str, SourceState]) -> SourceState:
        if table_key in ("dim_health_board", "dim_drug", "dim_prescribed_type", "fact_prescribing_monthly"):
            return states["hb_pitc"]
        if table_key == "dim_device_classification":
            return states["device_classification"]
        if table_key in ("fact_event_daily", "dim_user"):
            return states["raw_logs"] if states["raw_logs"].records_read else states["raw_entities"]
        combined = SourceState(name="combined")
        for state in states.values():
            combined.records_read += state.records_read
            combined.records_output += state.records_output
            combined.records_rejected += state.records_rejected
            combined.records_deduplicated += state.records_deduplicated
            combined.records_excluded += state.records_excluded
            combined.records_modified += state.records_modified
        return combined

    @staticmethod
    def _parent_run_id(states: dict[str, SourceState]) -> str | None:
        for name in ("raw_logs", "raw_entities", "hb_pitc", "device_classification"):
            if states[name].parent_run_id:
                return states[name].parent_run_id
        return None

    @staticmethod
    def _parse_business_date(value: str | date) -> date:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        return date.fromisoformat(str(value))

    def _resolve_business_date(self, value: str | date | None) -> date:
        if value is None:
            partitions = self._partitions("hb_pitc") or self._partitions("raw_logs")
            if not partitions:
                raise ValueError(
                    "No business_date supplied and no staging partitions were found; "
                    "run extraction first or pass --business-date."
                )
            return partitions[-1][0]
        return self._parse_business_date(value)


def to_json(results: list[TransformationResult]) -> str:
    payload = [
        {
            "table_name": r.table_name,
            "run_id": r.run_id,
            "decision": r.decision,
            "status": r.status,
            "records_read": r.records_read,
            "records_output": r.records_output,
            "records_rejected": r.records_rejected,
            "records_deduplicated": r.records_deduplicated,
            "records_modified": r.records_modified,
            "rules_applied": r.rules_applied,
            "output_path": r.output_path,
            "quarantine_path": r.quarantine_path,
            "message": r.message,
            "validation_results": r.check_map,
        }
        for r in results
    ]
    return json.dumps(payload, indent=2)
