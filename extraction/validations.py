"""Extraction validation and data quality checks (Section 3 - V1..V7).

Each check returns a :class:`CheckResult` capturing the check id, target,
criteria, severity, status and a human readable detail string. The extraction
pipeline consumes these results to produce the handover decision (Section 7).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl

from extraction.config import TableSpec

PASS = "PASS"
FAIL = "FAIL"
CRITICAL = "Critical"
WARNING = "Warning"

# Section 3 - static registry of check metadata.
CHECK_REGISTRY: dict[str, dict[str, str]] = {
    "V1": {
        "name": "Source File Access",
        "target": "Raw CSV/JSON files",
        "criteria": "Source files exist and are readable in storage path.",
        "severity": CRITICAL,
    },
    "V2": {
        "name": "Schema Drift Validation",
        "target": "Source Files",
        "criteria": "All required columns exist with matching data types during Polars parse.",
        "severity": CRITICAL,
    },
    "V3": {
        "name": "Window Resolution",
        "target": "Business Date",
        "criteria": "business_date is valid and no prior successful run exists in audit log.",
        "severity": CRITICAL,
    },
    "V4": {
        "name": "Key Integrity",
        "target": "Primary Keys",
        "criteria": "Zero Null values in Primary Keys for raw_logs and raw_entities.",
        "severity": CRITICAL,
    },
    "V5": {
        "name": "Primary Key Uniqueness",
        "target": "Primary Keys",
        "criteria": "count(distinct log_id) == count(log_id).",
        "severity": CRITICAL,
    },
    "V6": {
        "name": "Extraction Parquet Parity",
        "target": "Read vs Written Rows",
        "criteria": "rows_read == rows_extracted + rows_rejected.",
        "severity": CRITICAL,
    },
    "V7": {
        "name": "Non-Empty File Check",
        "target": "raw_logs",
        "criteria": "At least 1 log entry present in current extraction window.",
        "severity": WARNING,
    },
}


@dataclass
class CheckResult:
    check_id: str
    name: str
    target: str
    criteria: str
    severity: str
    status: str
    detail: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "check_id": self.check_id,
            "name": self.name,
            "criteria": self.criteria,
            "severity": self.severity,
            "status": self.status,
            "detail": self.detail,
        }


def _result(check_id: str, status: str, detail: str = "") -> CheckResult:
    meta = CHECK_REGISTRY[check_id]
    return CheckResult(
        check_id=check_id,
        name=meta["name"],
        target=meta["target"],
        criteria=meta["criteria"],
        severity=meta["severity"],
        status=status,
        detail=detail,
    )


# --------------------------------------------------------------------------- #
# V1 - Source File Access
# --------------------------------------------------------------------------- #
def check_source_file_access(files: list[Path]) -> CheckResult:
    if not files:
        return _result("V1", FAIL, "No source CSV/JSON files found in storage path.")
    unreadable = [str(f) for f in files if not f.is_file()]
    if unreadable:
        return _result("V1", FAIL, f"Unreadable source files: {unreadable}")
    return _result("V1", PASS, f"{len(files)} source file(s) readable.")


# --------------------------------------------------------------------------- #
# V2 - Schema Drift Validation
# --------------------------------------------------------------------------- #
def _is_string_like(dtype: pl.DataType) -> bool:
    return dtype in (pl.String, pl.Categorical, pl.Enum)


def _is_temporal(dtype: pl.DataType) -> bool:
    return dtype in (pl.Date, pl.Datetime)


def check_schema_drift(
    schema: pl.Schema,
    spec: TableSpec,
    timestamp_parse_rate: float | None,
) -> CheckResult:
    missing: list[str] = []
    drifted: list[str] = []

    for column, kind in spec.required_columns.items():
        if column not in schema:
            missing.append(column)
            continue
        dtype = schema[column]
        if kind == "string":
            if not _is_string_like(dtype):
                # Untyped sources that infer a numeric key are schema drift.
                drifted.append(f"{column}: expected String, got {dtype}")
        else:  # timestamp
            if _is_temporal(dtype):
                continue
            if _is_string_like(dtype) and (timestamp_parse_rate or 0.0) >= 1.0:
                continue
            drifted.append(
                f"{column}: expected Timestamp, got {dtype}"
                + (f" (parse rate {timestamp_parse_rate:.0%})" if timestamp_parse_rate is not None else "")
            )

    if missing:
        return _result("V2", FAIL, f"Missing required columns: {missing}")
    if drifted:
        return _result("V2", FAIL, "Schema drift: " + "; ".join(drifted))
    return _result("V2", PASS, "All required columns present with matching types.")


# --------------------------------------------------------------------------- #
# V3 - Window Resolution
# --------------------------------------------------------------------------- #
def check_window_resolution(
    business_date: date,
    prior_successful_run: bool,
    force: bool = False,
) -> CheckResult:
    if business_date > date.today():
        return _result("V3", FAIL, f"business_date {business_date} is in the future.")
    if prior_successful_run and not force:
        return _result(
            "V3",
            FAIL,
            f"A prior successful run already exists for {business_date} (use --force to overwrite).",
        )
    if prior_successful_run and force:
        return _result(
            "V3",
            PASS,
            f"business_date {business_date} valid; prior run overwritten (force).",
        )
    return _result("V3", PASS, f"business_date {business_date} resolved with no prior successful run.")


# --------------------------------------------------------------------------- #
# V4 - Key Integrity
# --------------------------------------------------------------------------- #
def check_key_integrity(df: pl.DataFrame, primary_key: str) -> CheckResult:
    if primary_key not in df.columns:
        return _result("V4", FAIL, f"Primary key {primary_key} missing from parsed data.")
    null_count = df.get_column(primary_key).null_count()
    if null_count > 0:
        return _result("V4", FAIL, f"{null_count} null value(s) in primary key {primary_key}.")
    return _result("V4", PASS, f"No null values in primary key {primary_key}.")


# --------------------------------------------------------------------------- #
# V5 - Primary Key Uniqueness
# --------------------------------------------------------------------------- #
def check_primary_key_uniqueness(df: pl.DataFrame, primary_key: str) -> CheckResult:
    if primary_key not in df.columns:
        return _result("V5", FAIL, f"Primary key {primary_key} missing from parsed data.")
    total = df.height
    distinct = df.get_column(primary_key).n_unique()
    if distinct != total:
        return _result(
            "V5",
            FAIL,
            f"Duplicate primary keys detected: {total - distinct} duplicate row(s) in {primary_key}.",
        )
    return _result("V5", PASS, f"All {total} {primary_key} values are unique.")


# --------------------------------------------------------------------------- #
# V6 - Extraction Parquet Parity
# --------------------------------------------------------------------------- #
def check_parquet_parity(
    rows_read: int,
    rows_extracted: int,
    rows_rejected: int,
    written_rows: int,
) -> CheckResult:
    if rows_read != rows_extracted + rows_rejected:
        return _result(
            "V6",
            FAIL,
            f"Parity mismatch: read={rows_read}, extracted+rejected={rows_extracted + rows_rejected}.",
        )
    if written_rows != rows_extracted:
        return _result(
            "V6",
            FAIL,
            f"Parquet write mismatch: expected {rows_extracted} rows, wrote {written_rows}.",
        )
    return _result(
        "V6",
        PASS,
        f"rows_read({rows_read}) == rows_extracted({rows_extracted}) + rows_rejected({rows_rejected}).",
    )


# --------------------------------------------------------------------------- #
# V7 - Non-Empty File Check
# --------------------------------------------------------------------------- #
def check_non_empty(df: pl.DataFrame, table_name: str) -> CheckResult:
    if df.height == 0:
        return _result("V7", FAIL, f"No rows extracted for {table_name} in the current window.")
    return _result("V7", PASS, f"{df.height} row(s) present for {table_name}.")
