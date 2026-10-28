"""Acceptance decisions, partitioned output writing and handover manifest."""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime
from pathlib import Path

import polars as pl

from transformation.constants import (
    APPROVED,
    CRITICAL,
    FAIL,
    MANUAL_REVIEW,
    NON_CRITICAL,
    REJECTED,
    WARNING,
    date_from_key,
    iso,
)
from transformation.models import CheckResult, SourceState, TransformationResult


class HandoverMixin:
    """Decide acceptance and publish validated outputs to the Load handover."""

    def _decide(self, states: dict[str, SourceState], checks: list[CheckResult]) -> str:
        total_read = sum(state.records_read for state in states.values())
        total_rejected = sum(state.records_rejected for state in states.values())
        ratio = (total_rejected / total_read) if total_read else 0.0
        critical_fail = any(c.status == FAIL and c.severity == CRITICAL for c in checks)
        warning_fail = any(c.status == WARNING or (c.status == FAIL and c.severity == NON_CRITICAL) for c in checks)
        if critical_fail or ratio > self.settings.max_rejected_ratio_threshold:
            return REJECTED
        if warning_fail or ratio > self.settings.rejected_ratio_threshold:
            return MANUAL_REVIEW
        return APPROVED

    @staticmethod
    def _status_for(decision: str) -> str:
        if decision == APPROVED:
            return "SUCCESS"
        if decision == MANUAL_REVIEW:
            return "MANUAL_REVIEW"
        return "REJECTED"

    @staticmethod
    def _write_partition(frame: pl.DataFrame, table_dir: Path, partition: str | None = None) -> Path:
        target = table_dir / f"business_date={partition}" if partition else table_dir
        target.mkdir(parents=True, exist_ok=True)
        tmp = target / f".tmp_{uuid.uuid4().hex[:8]}.parquet"
        frame.write_parquet(tmp, compression="zstd")
        final = target / "data.parquet"
        tmp.replace(final)
        return final

    def _affected_partitions(self, table_key: str, frame: pl.DataFrame, bdate: date) -> list[str]:
        if table_key.startswith("dim_"):
            return [bdate.replace(day=1).isoformat()]
        if table_key == "fact_prescribing_monthly":
            if frame.height:
                return sorted(month.isoformat() for month in frame["paid_month"].unique().to_list())
            return [bdate.replace(day=1).isoformat()]
        if frame.height:
            return sorted({date_from_key(key).isoformat() for key in frame["event_date_key"].unique().to_list()})
        return [bdate.isoformat()]

    def _existing_output(self, table_key: str, table_dir: Path, partitions: list[str]) -> Path | None:
        if table_key.startswith("dim_"):
            path = table_dir / f"business_date={partitions[0]}" / "data.parquet"
            return path if path.exists() else None
        if any((table_dir / f"business_date={p}" / "data.parquet").exists() for p in partitions):
            return table_dir
        return None

    def _write_outputs(
        self,
        transformed: dict[str, pl.DataFrame],
        bdate: date,
        force: bool = False,
        batch_id: str | None = None,
    ) -> dict[str, Path]:
        paths: dict[str, Path] = {}
        for table_key, frame in transformed.items():
            table_name = f"analytics.{table_key}"
            table_dir = self.settings.transformed_dir_for(table_key)
            partitions = self._affected_partitions(table_key, frame, bdate)
            existing = self._existing_output(table_key, table_dir, partitions)
            if (
                not force
                and batch_id
                and existing is not None
                and self.store.has_successful_run(batch_id, table_name)
            ):
                paths[table_key] = existing
                continue

            for partition in partitions:
                for stale in (table_dir / f"business_date={partition}").glob("data.parquet"):
                    stale.unlink(missing_ok=True)

            # Remove artifacts from the pre-partitioned output layout.
            if table_key.startswith("dim_"):
                (table_dir / "data.parquet").unlink(missing_ok=True)
            else:
                for stale in (table_dir / "business_date=1900-01-01").glob("data.parquet"):
                    stale.unlink(missing_ok=True)

            if table_key.startswith("dim_"):
                paths[table_key] = self._write_partition(frame, table_dir, partitions[0])
            elif table_key == "fact_prescribing_monthly":
                if frame.height:
                    for month in sorted(frame["paid_month"].unique().to_list()):
                        part = frame.filter(pl.col("paid_month") == month)
                        self._write_partition(part, table_dir, month.isoformat())
                else:
                    self._write_partition(frame, table_dir, partitions[0])
                paths[table_key] = table_dir
            else:
                if frame.height:
                    for event_date in sorted(frame["event_date_key"].unique().to_list()):
                        part = frame.filter(pl.col("event_date_key") == event_date)
                        self._write_partition(part, table_dir, date_from_key(event_date).isoformat())
                else:
                    self._write_partition(frame, table_dir, partitions[0])
                paths[table_key] = table_dir
        return paths

    def _write_manifest(
        self,
        results: list[TransformationResult],
        states: dict[str, SourceState],
        bdate: date,
        batch_id: str,
        parent_run_id: str | None,
        started: datetime,
        ended: datetime,
        duration_ms: int,
    ) -> Path:
        quarantine_manifest: dict[str, dict[str, int]] = {}
        for name, state in states.items():
            counts: dict[str, int] = {}
            for frame in state.rejected_frames:
                if frame.height == 0 or "rule_id" not in frame.columns:
                    continue
                for rule_id, count in frame.group_by("rule_id").len().iter_rows():
                    counts[rule_id] = counts.get(rule_id, 0) + int(count)
            if counts:
                quarantine_manifest[name] = counts

        manifest = {
            "run_id": results[0].run_id if results else None,
            "parent_run_id": parent_run_id,
            "batch_id": batch_id,
            "batch_window_start": bdate.isoformat(),
            "batch_window_end": bdate.isoformat(),
            "business_date": bdate.isoformat(),
            "decision": results[0].decision if results else None,
            "status": results[0].status if results else None,
            "transformation_version": self.settings.transformation_version,
            "transformation_started_at": iso(started),
            "transformation_ended_at": iso(ended),
            "duration_ms": duration_ms,
            "tables": [
                {
                    "table_name": result.table_name,
                    "records_read": result.records_read,
                    "records_output": result.records_output,
                    "records_rejected": result.records_rejected,
                    "records_deduplicated": result.records_deduplicated,
                    "records_modified": result.records_modified,
                    "rules_applied": result.rules_applied,
                    "output_path": result.output_path,
                    "quarantine_path": result.quarantine_path,
                }
                for result in results
            ],
            "quarantine_manifest": quarantine_manifest,
            "validation_results": results[0].check_map if results else {},
        }

        handover_dir = Path(self.settings.transformed_dir) / "_handover" / f"business_date={bdate.isoformat()}"
        handover_dir.mkdir(parents=True, exist_ok=True)
        target = handover_dir / "manifest.json"
        tmp = handover_dir / f".tmp_{uuid.uuid4().hex[:8]}.json"
        tmp.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
        tmp.replace(target)
        return target

    def _write_quarantine(
        self,
        states: dict[str, SourceState],
        bdate: date,
        run_id: str,
        batch_id: str,
    ) -> dict[str, Path]:
        paths: dict[str, Path] = {}
        for name, state in states.items():
            frames = [f for f in state.rejected_frames if f.height]
            if not frames:
                continue
            rejected = pl.concat(frames, how="diagonal_relaxed")
            target_dir = self.settings.quarantine_dir_for(name) / f"business_date={bdate.isoformat()}"
            target_dir.mkdir(parents=True, exist_ok=True)
            path = target_dir / "rejected.parquet"
            tmp = target_dir / f".tmp_{uuid.uuid4().hex[:8]}.parquet"
            rejected.write_parquet(tmp, compression="zstd")
            tmp.replace(path)
            paths[name] = path
        return paths
