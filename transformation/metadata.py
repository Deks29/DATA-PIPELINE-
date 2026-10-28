"""Transformation metadata, audit records and local JSON logs (Stage 2, Section 8)."""

from __future__ import annotations

import json
import re
import shutil
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

SUCCESSFUL_DECISIONS = ("APPROVED", "APPROVED WITH WARNING")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS transformation_runs (
    run_id                 TEXT NOT NULL,
    parent_run_id          TEXT,
    batch_id               TEXT NOT NULL,
    batch_window_start     TEXT,
    batch_window_end       TEXT,
    table_name             TEXT NOT NULL,
    transformation_started_at TEXT,
    transformation_ended_at   TEXT,
    duration_ms            INTEGER,
    status                 TEXT,
    decision               TEXT,
    records_read           INTEGER,
    records_output         INTEGER,
    records_rejected       INTEGER,
    records_deduplicated   INTEGER,
    records_excluded       INTEGER,
    records_modified       INTEGER,
    rules_applied          TEXT,
    validation_results     TEXT,
    error_code             TEXT,
    error_message          TEXT,
    transformation_version TEXT,
    created_at             TEXT NOT NULL,
    PRIMARY KEY (run_id, table_name)
);
CREATE INDEX IF NOT EXISTS idx_transform_lookup
    ON transformation_runs (batch_id, table_name, decision);
"""


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime | None) -> str | None:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if dt else None


@dataclass
class TransformationMetadata:
    """Audit record for one transformed output table."""

    run_id: str
    parent_run_id: str | None
    batch_id: str
    batch_window_start: str
    batch_window_end: str
    table_name: str
    transformation_started_at: str | None
    transformation_ended_at: str | None
    duration_ms: int
    status: str
    decision: str
    records_read: int
    records_output: int
    records_rejected: int
    records_deduplicated: int
    records_excluded: int
    records_modified: int
    rules_applied: dict[str, int] = field(default_factory=dict)
    validation_results: dict[str, str] = field(default_factory=dict)
    error_code: str | None = None
    error_message: str | None = None
    transformation_version: str = "2.0.0"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


_PARTITION_RE = re.compile(r"business_date=(\d{4}-\d{2}-\d{2})$")


class TransformMetadataStore:
    """SQLite-backed transformation audit table plus local JSON log writer."""

    def __init__(self, db_path: Path, logs_dir: Path, quarantine_dir: Path | None = None) -> None:
        self.db_path = Path(db_path)
        self.logs_dir = Path(logs_dir)
        self.quarantine_dir = Path(quarantine_dir) if quarantine_dir else None
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(_SCHEMA)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def record(self, metadata: TransformationMetadata) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO transformation_runs (
                    run_id, parent_run_id, batch_id, batch_window_start,
                    batch_window_end, table_name, transformation_started_at,
                    transformation_ended_at, duration_ms, status, decision,
                    records_read, records_output, records_rejected,
                    records_deduplicated, records_excluded, records_modified,
                    rules_applied, validation_results, error_code, error_message,
                    transformation_version, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (run_id, table_name) DO UPDATE SET
                    status                 = excluded.status,
                    decision               = excluded.decision,
                    transformation_ended_at = excluded.transformation_ended_at,
                    duration_ms            = excluded.duration_ms,
                    records_read           = excluded.records_read,
                    records_output         = excluded.records_output,
                    records_rejected       = excluded.records_rejected,
                    records_deduplicated   = excluded.records_deduplicated,
                    records_excluded       = excluded.records_excluded,
                    records_modified       = excluded.records_modified,
                    rules_applied          = excluded.rules_applied,
                    validation_results     = excluded.validation_results,
                    error_code             = excluded.error_code,
                    error_message          = excluded.error_message
                """,
                (
                    metadata.run_id,
                    metadata.parent_run_id,
                    metadata.batch_id,
                    metadata.batch_window_start,
                    metadata.batch_window_end,
                    metadata.table_name,
                    metadata.transformation_started_at,
                    metadata.transformation_ended_at,
                    metadata.duration_ms,
                    metadata.status,
                    metadata.decision,
                    metadata.records_read,
                    metadata.records_output,
                    metadata.records_rejected,
                    metadata.records_deduplicated,
                    metadata.records_excluded,
                    metadata.records_modified,
                    json.dumps(metadata.rules_applied),
                    json.dumps(metadata.validation_results),
                    metadata.error_code,
                    metadata.error_message,
                    metadata.transformation_version,
                    _iso(utcnow()),
                ),
            )
        self._write_json_log(metadata)

    def _write_json_log(self, metadata: TransformationMetadata) -> None:
        day = str(metadata.batch_window_end or date.today().isoformat())
        day_dir = self.logs_dir / day
        day_dir.mkdir(parents=True, exist_ok=True)
        safe_table = metadata.table_name.replace(".", "_")
        log_file = day_dir / f"{metadata.run_id}_{safe_table}.json"
        log_file.write_text(
            json.dumps(metadata.to_dict(), indent=2, default=str),
            encoding="utf-8",
        )

    def has_successful_run(self, batch_id: str, table_name: str) -> bool:
        placeholders = ", ".join("?" for _ in SUCCESSFUL_DECISIONS)
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT 1 FROM transformation_runs
                WHERE batch_id = ? AND table_name = ?
                  AND decision IN ({placeholders})
                LIMIT 1
                """,
                (batch_id, table_name, *SUCCESSFUL_DECISIONS),
            ).fetchone()
        return row is not None

    def get_run(self, run_id: str, table_name: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM transformation_runs WHERE run_id = ? AND table_name = ?",
                (run_id, table_name),
            ).fetchone()
        return dict(row) if row else None

    def get_batch(self, batch_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM transformation_runs WHERE batch_id = ? ORDER BY table_name",
                (batch_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def purge_expired(
        self,
        log_retention_days: int = 90,
        audit_retention_months: int = 24,
        quarantine_retention_months: int = 24,
    ) -> dict[str, int]:
        removed_rows = 0
        with self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM transformation_runs WHERE created_at < datetime('now', ?)",
                (f"-{audit_retention_months} months",),
            )
            removed_rows = cur.rowcount

        removed_files = 0
        cutoff = utcnow().timestamp() - log_retention_days * 86400
        if self.logs_dir.exists():
            for child in self.logs_dir.iterdir():
                if child.is_dir() and child.stat().st_mtime < cutoff:
                    for path in child.glob("*.json"):
                        path.unlink(missing_ok=True)
                        removed_files += 1

        removed_partitions = 0
        if self.quarantine_dir and self.quarantine_dir.exists():
            cutoff_date = (utcnow() - timedelta(days=30 * quarantine_retention_months)).date()
            for partition in list(self.quarantine_dir.rglob("business_date=*")):
                match = _PARTITION_RE.search(partition.name)
                if not match or not partition.is_dir():
                    continue
                if date.fromisoformat(match.group(1)) < cutoff_date:
                    shutil.rmtree(partition)
                    removed_partitions += 1
        return {
            "audit_rows_removed": removed_rows,
            "log_files_removed": removed_files,
            "quarantine_partitions_removed": removed_partitions,
        }
