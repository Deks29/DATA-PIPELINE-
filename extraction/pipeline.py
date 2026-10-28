"""Polars extraction pipeline for the SightSync batch pipeline (Stage 1).

Reads raw CSV/JSON log drops as Polars LazyFrames, resolves the incremental
business-date window (with a trailing look-back for late logs), runs the V1-V7
validation suite, and writes compressed Parquet staging files partitioned by
``business_date``. A full audit record is written for every table extraction.
"""

from __future__ import annotations

import json
import shutil
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import polars as pl

from extraction.api import fetch_to_ndjson
from extraction.config import SUPPORTED_EXTENSIONS, TABLES, Settings, TableSpec, load_settings
from extraction.metadata import ExtractionMetadata, MetadataStore, utcnow
from extraction.validations import (
    CRITICAL,
    FAIL,
    WARNING,
    CheckResult,
    check_key_integrity,
    check_non_empty,
    check_parquet_parity,
    check_primary_key_uniqueness,
    check_schema_drift,
    check_source_file_access,
    check_window_resolution,
)

SKIP = "SKIP"

_TZ_SUFFIX = r"\s*([Zz]|[+-]\d{2}:?\d{2}|UTC)$"
_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S%.f"


def _parse_timestamp_string(column: str) -> pl.Expr:
    """Normalise heterogeneous ISO timestamp strings to tz-naive Datetime[us].

    Handles mixed representations produced when CSV (Datetime -> String with a
    space separator and ``+00:00`` offset) and JSON (``T`` separator, ``Z``
    suffix) sources are concatenated.
    """
    return (
        pl.col(column)
        .cast(pl.String)
        .str.strip_chars()
        .str.replace_all(r"[Tt]", " ")
        .str.replace(_TZ_SUFFIX, "")
        .str.to_datetime(format=_DATETIME_FORMAT, strict=False, time_unit="us")
    )

def _yyyymm_expr(column: str) -> pl.Expr:
    """Convert an integer ``YYYYMM`` period to a UTC datetime at month start."""
    return (
        pl.col(column)
        .cast(pl.String)
        .str.to_date(format="%Y%m", strict=False)
        .cast(pl.Datetime("us"))
        .dt.replace_time_zone("UTC")
        .alias(column)
    )


def _date_expr(column: str) -> pl.Expr:
    """Convert a ``YYYY-MM-DD`` date string to a UTC datetime at midnight."""
    return (
        pl.col(column)
        .cast(pl.String)
        .str.strip_chars()
        .str.to_datetime(format="%Y-%m-%d", strict=False, time_unit="us")
        .dt.replace_time_zone("UTC")
        .alias(column)
    )


APPROVED = "APPROVED"
APPROVED_WITH_WARNING = "APPROVED WITH WARNING"
REJECTED = "REJECTED"
MANUAL_REVIEW = "MANUAL REVIEW"
SKIPPED = "SKIPPED"


@dataclass
class ExtractionResult:
    """Outcome of extracting a single staging table."""

    table_name: str
    table_key: str
    run_id: str
    decision: str
    checks: list[CheckResult] = field(default_factory=list)
    rows_read: int = 0
    rows_extracted: int = 0
    rows_rejected: int = 0
    parquet_path: str | None = None
    message: str = ""

    @property
    def check_map(self) -> dict[str, str]:
        return {c.check_id: c.status for c in self.checks}

    @property
    def is_success(self) -> bool:
        return self.decision in (APPROVED, APPROVED_WITH_WARNING, SKIPPED)


class ExtractionPipeline:
    """Orchestrates Stage 1 extraction for one or more staging tables."""

    def __init__(self, settings: Settings | None = None, store: MetadataStore | None = None) -> None:
        self.settings = settings or load_settings()
        self.store = store or MetadataStore(self.settings.metadata_db, self.settings.logs_dir)

    # -- public API -----------------------------------------------------------

    def run(
        self,
        business_date: str | date,
        tables: list[str] | None = None,
        force: bool = False,
        run_id: str | None = None,
    ) -> list[ExtractionResult]:
        """Extract all (or selected) tables for ``business_date``.

        Returns one :class:`ExtractionResult` per table. A table with a prior
        successful run is skipped idempotently unless ``force`` is set.
        """

        bdate = self._parse_business_date(business_date)
        run_id = run_id or str(uuid.uuid4())
        run_key = f"{self.settings.pipeline_name}:{bdate.isoformat()}"
        table_keys = tables or list(TABLES)

        results: list[ExtractionResult] = []
        for key in table_keys:
            if key not in TABLES:
                raise ValueError(f"Unknown table '{key}'. Expected one of {list(TABLES)}")
            spec = TABLES[key]
            if not force and self.store.has_successful_run(
                self.settings.pipeline_name, bdate, spec.name
            ):
                results.append(
                    ExtractionResult(
                        table_name=spec.name,
                        table_key=key,
                        run_id=run_id,
                        decision=SKIPPED,
                        message=f"Idempotent skip: successful run already exists for {bdate}.",
                    )
                )
                continue
            results.append(self._extract_table(run_id, run_key, bdate, key, spec, force))
        return results

    def extract(
        self, business_date: str | date, table: str, force: bool = False
    ) -> ExtractionResult:
        return self.run(business_date, tables=[table], force=force)[0]

    # -- extraction per table -------------------------------------------------

    def _extract_table(
        self,
        run_id: str,
        run_key: str,
        bdate: date,
        key: str,
        spec: TableSpec,
        force: bool,
    ) -> ExtractionResult:
        started = utcnow()
        checks: list[CheckResult] = []
        rows_read = rows_extracted = rows_rejected = 0
        parquet_path: Path | None = None
        message = ""

        try:
            files = self._discover_files(spec)
            checks.append(check_source_file_access(files))

            if checks[-1].status == FAIL:
                checks.extend(self._skip_rest("V1", "Upstream source file check failed."))
                decision = REJECTED
                message = "No readable source files."
            else:
                decision, checks, rows_read, rows_extracted, rows_rejected, parquet_path, message = (
                    self._extract_validated(run_id, bdate, key, spec, files, force, checks)
                )
        except Exception as exc:  # noqa: BLE001 - audit every failure
            checks.extend(self._skip_rest(checks[-1].check_id if checks else "", str(exc)))
            decision = REJECTED
            message = f"Extraction error: {exc}"

        ended = utcnow()
        metadata = ExtractionMetadata(
            run_id=run_id,
            run_key=run_key,
            pipeline_name=self.settings.pipeline_name,
            business_date=bdate.isoformat(),
            table_name=spec.name,
            extraction_started_at=_iso(started),
            extraction_ended_at=_iso(ended),
            rows_read=rows_read,
            rows_extracted=rows_extracted,
            rows_rejected=rows_rejected,
            validation_results={c.check_id: c.status for c in checks},
            decision=decision,
        )
        self.store.record(metadata)

        return ExtractionResult(
            table_name=spec.name,
            table_key=key,
            run_id=run_id,
            decision=decision,
            checks=checks,
            rows_read=rows_read,
            rows_extracted=rows_extracted,
            rows_rejected=rows_rejected,
            parquet_path=str(parquet_path) if parquet_path else None,
            message=message,
        )

    def _extract_validated(
        self,
        run_id: str,
        bdate: date,
        key: str,
        spec: TableSpec,
        files: list[Path],
        force: bool,
        checks: list[CheckResult] | None = None,
    ):
        checks = checks if checks is not None else []

        # -- V2: parse schema and detect drift --------------------------------
        lf = self._scan(files)
        schema = lf.collect_schema()
        parse_rate = self._timestamp_parse_rate(lf, spec, schema)
        checks.append(check_schema_drift(schema, spec, parse_rate))

        # -- V3: window resolution --------------------------------------------
        prior = self.store.has_successful_run(self.settings.pipeline_name, bdate, spec.name)
        checks.append(check_window_resolution(bdate, prior, force))

        if any(c.status == FAIL for c in checks if c.severity == CRITICAL):
            checks.extend(self._skip_rest("V3", "Schema/window validation failed before read."))
            decision = REJECTED
            return decision, checks, 0, 0, 0, None, "Aborted after V2/V3."

        # -- read, normalise and apply the incremental window -----------------
        df = self._normalise(lf, spec)
        window_start, window_end = self._window(bdate, spec)
        ts_col = spec.timestamp_column
        in_window = df.filter(
            pl.col(ts_col).is_not_null()
            & (pl.col(ts_col) >= window_start)
            & (pl.col(ts_col) <= window_end)
        )
        rows_read = in_window.height

        valid_mask = pl.col(ts_col).is_not_null()
        if not spec.allow_null_keys:
            valid_mask = valid_mask & pl.all_horizontal(
                [pl.col(key).is_not_null() for key in spec.primary_keys]
            )
        valid = in_window.filter(valid_mask)
        rows_rejected = rows_read - valid.height
        rows_extracted = valid.height

        # -- V4 / V5 ----------------------------------------------------------
        # V4 checks the raw in-window source (key integrity); V5 checks the
        # de-duplicated/cleaned rows that will actually be extracted.
        checks.append(
            check_key_integrity(
                in_window, spec.primary_keys, allow_nulls=spec.allow_null_keys
            )
        )
        checks.append(check_primary_key_uniqueness(valid, spec.primary_keys))

        critical_ok = not any(c.status == FAIL and c.severity == CRITICAL for c in checks)
        written_rows = 0
        parquet_path: Path | None = None

        if critical_ok:
            parquet_path, written_rows = self._write_parquet(valid, spec, bdate, run_id)
            checks.append(
                check_parquet_parity(rows_read, rows_extracted, rows_rejected, written_rows)
            )
        else:
            checks.append(self._skip("V6", "Parquet write skipped due to upstream critical failure."))
            if parquet_path is not None:
                shutil.rmtree(parquet_path.parent, ignore_errors=True)

        # -- V7 ---------------------------------------------------------------
        checks.append(check_non_empty(valid, spec.name))

        decision = decide(checks, rows_read, rows_rejected, self.settings.rejected_ratio_threshold)

        if decision == REJECTED and parquet_path is not None:
            # Section 7 - REJECTED halts the pipeline; do not hand off bad data.
            shutil.rmtree(parquet_path.parent, ignore_errors=True)
            parquet_path = None

        return decision, checks, rows_read, rows_extracted, rows_rejected, parquet_path, ""

    # -- I/O helpers ----------------------------------------------------------

    def _discover_files(self, spec: TableSpec) -> list[Path]:
        if spec.source == "api":
            return self._fetch_api_source(spec)
        raw_dir = self.settings.raw_dir_for(spec.name.split(".")[-1])
        if not raw_dir.exists():
            return []
        files: list[Path] = []
        for ext in SUPPORTED_EXTENSIONS:
            files.extend(sorted(raw_dir.glob(spec.file_glob.format(ext=ext))))
        return files

    def _fetch_api_source(self, spec: TableSpec) -> list[Path]:
        """Pull the API snapshot into the raw directory and return its file(s)."""

        table_key = spec.name.split(".")[-1]
        out_path = self.settings.raw_dir_for(table_key) / f"{table_key}.json"
        path, _, _ = fetch_to_ndjson(
            base_url=self.settings.api_url,
            out_path=out_path,
            string_fields=spec.string_columns,
            search=spec.api_search or self.settings.api_search,
            limit=self.settings.api_limit,
            api_key=self.settings.api_key or None,
        )
        return [path]

    def _scan(self, files: list[Path]) -> pl.LazyFrame:
        frames: list[pl.LazyFrame] = []
        for path in files:
            ext = path.suffix.lower().lstrip(".")
            if ext == "csv":
                frames.append(
                    pl.scan_csv(path, infer_schema_length=10_000, try_parse_dates=True)
                )
            elif ext == "json":
                frames.append(pl.scan_ndjson(path, infer_schema_length=10_000))
            else:
                raise ValueError(f"Unsupported source extension: {path}")
        if len(frames) == 1:
            return frames[0]
        return pl.concat(frames, how="diagonal_relaxed")

    def _timestamp_parse_rate(
        self, lf: pl.LazyFrame, spec: TableSpec, schema: pl.Schema
    ) -> float | None:
        """Fraction of non-null values parseable as datetimes (None if temporal)."""

        if spec.timestamp_format == "yyyymm":
            parsed = lf.select(_yyyymm_expr(spec.timestamp_column).is_not_null().mean()).collect()
            return float(parsed.item() or 0.0)

        if spec.timestamp_format == "date":
            parsed = lf.select(_date_expr(spec.timestamp_column).is_not_null().mean()).collect()
            return float(parsed.item() or 0.0)

        string_ts = [c for c in spec.timestamp_columns if schema.get(c) == pl.String]
        if not string_ts:
            return None
        rates: list[float] = []
        for column in string_ts:
            parsed = lf.select(
                _parse_timestamp_string(column).is_not_null().mean()
            ).collect()
            rates.append(float(parsed.item() or 0.0))
        return min(rates) if rates else None

    def _normalise(self, lf: pl.LazyFrame, spec: TableSpec) -> pl.DataFrame:
        df = lf.collect()
        exprs: list[pl.Expr] = []
        for column in spec.string_columns:
            if column in df.columns:
                exprs.append(pl.col(column).cast(pl.String))
        for column in spec.timestamp_columns:
            if column in df.columns:
                if spec.timestamp_format == "yyyymm":
                    exprs.append(_yyyymm_expr(column))
                elif spec.timestamp_format == "date":
                    exprs.append(_date_expr(column))
                else:
                    exprs.append(self._timestamp_expr(column, df.schema[column]))
        if exprs:
            # keep only contracted columns + control-ready ones
            keep = [c for c in spec.required_columns if c in df.columns]
            df = df.select(keep).with_columns(exprs)
        return df

    @staticmethod
    def _timestamp_expr(column: str, dtype: pl.DataType) -> pl.Expr:
        """Standardise a timestamp column to a tz-aware UTC Datetime[us]."""
        col = pl.col(column)
        if dtype == pl.String or dtype in (pl.Categorical, pl.Enum):
            col = _parse_timestamp_string(column).dt.replace_time_zone("UTC")
        elif dtype == pl.Date:
            col = col.cast(pl.Datetime("us")).dt.replace_time_zone("UTC")
        elif dtype == pl.Datetime:
            if getattr(dtype, "time_zone", None):
                col = col.dt.convert_time_zone("UTC")
            else:
                col = col.dt.replace_time_zone("UTC")
        else:
            col = col.cast(pl.Datetime("us")).dt.replace_time_zone("UTC")
        return col.dt.cast_time_unit("us").alias(column)

    def _window(self, bdate: date, spec: TableSpec) -> tuple[datetime, datetime]:
        if spec.window_mode == "month":
            start_day = bdate.replace(day=1)
            next_month = (start_day.replace(day=28) + timedelta(days=4)).replace(day=1)
            start = datetime.combine(start_day, time.min).replace(tzinfo=timezone.utc)
            end = datetime.combine(
                next_month - timedelta(days=1), time.max
            ).replace(tzinfo=timezone.utc)
            return start, end
        start_day = bdate - timedelta(days=self.settings.lookback_days)
        start = datetime.combine(start_day, time.min).replace(tzinfo=timezone.utc)
        end = datetime.combine(bdate, time.max).replace(tzinfo=timezone.utc)
        return start, end

    def _write_parquet(
        self, df: pl.DataFrame, spec: TableSpec, bdate: date, run_id: str
    ) -> tuple[Path, int]:
        table_key = spec.name.split(".")[-1]
        table_dir = self.settings.staging_dir_for(table_key)
        partition_name = f"{spec.partition_column}={bdate.isoformat()}"
        target = table_dir / partition_name
        tmp = table_dir / f".tmp_{run_id}"
        if tmp.exists():
            shutil.rmtree(tmp, ignore_errors=True)
        tmp.mkdir(parents=True, exist_ok=True)

        # Section 6.1 - standard control columns.
        enriched = df.with_columns(
            pl.lit(run_id, dtype=pl.String).alias("run_id"),
            pl.lit(bdate).cast(pl.Date).alias(spec.partition_column),
            pl.lit(utcnow()).cast(pl.Datetime("us", "UTC")).alias("extracted_at"),
        )
        out_file = tmp / "data.parquet"
        enriched.write_parquet(out_file, compression="zstd")

        written = pl.read_parquet(out_file).height
        self._atomic_replace_dir(tmp, target)
        return target / "data.parquet", written

    @staticmethod
    def _atomic_replace_dir(src: Path, target: Path) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            backup = target.with_name(f"{target.name}.bak_{uuid.uuid4().hex[:8]}")
            target.rename(backup)
            try:
                src.rename(target)
            finally:
                shutil.rmtree(backup, ignore_errors=True)
        else:
            src.rename(target)

    # -- misc helpers ---------------------------------------------------------

    def _skip(self, check_id: str, detail: str) -> CheckResult:
        from extraction.validations import CHECK_REGISTRY

        meta = CHECK_REGISTRY[check_id]
        return CheckResult(
            check_id=check_id,
            name=meta["name"],
            target=meta["target"],
            criteria=meta["criteria"],
            severity=meta["severity"],
            status=SKIP,
            detail=detail,
        )

    def _skip_rest(self, after_id: str, detail: str) -> list[CheckResult]:
        order = ["V1", "V2", "V3", "V4", "V5", "V6", "V7"]
        start = order.index(after_id) + 1 if after_id in order else 0
        return [self._skip(cid, detail) for cid in order[start:]]

    @staticmethod
    def _parse_business_date(value: str | date) -> date:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        return date.fromisoformat(str(value))


# --------------------------------------------------------------------------- #
# Section 7 - Handover Acceptance and Decision Rules
# --------------------------------------------------------------------------- #
def decide(
    checks: list[CheckResult],
    rows_read: int,
    rows_rejected: int,
    rejected_ratio_threshold: float,
) -> str:
    critical_fail = any(c.status == FAIL and c.severity == CRITICAL for c in checks)
    warning_fail = any(c.status == FAIL and c.severity == WARNING for c in checks)

    if critical_fail:
        return REJECTED
    if rows_read and (rows_rejected / rows_read) > rejected_ratio_threshold:
        return MANUAL_REVIEW
    if warning_fail:
        return APPROVED_WITH_WARNING
    return APPROVED


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def to_json(results: list[ExtractionResult]) -> str:
    payload = [
        {
            "table_name": r.table_name,
            "run_id": r.run_id,
            "decision": r.decision,
            "rows_read": r.rows_read,
            "rows_extracted": r.rows_extracted,
            "rows_rejected": r.rows_rejected,
            "parquet_path": r.parquet_path,
            "message": r.message,
            "validation_results": r.check_map,
        }
        for r in results
    ]
    return json.dumps(payload, indent=2)
