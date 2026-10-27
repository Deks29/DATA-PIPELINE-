"""Extraction metadata and audit logging (Section 4 & 5 of the documentation).

Every extraction run writes:
  * one audit row into the metadata log table (SQLite stand-in for the Airflow
    metadata database), and
  * one local JSON log file under ``data/logs/<business_date>/``.

Retention rules from Section 5 are enforced by :meth:`MetadataStore.purge_expired`.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterator

SUCCESSFUL_DECISIONS = ("APPROVED", "APPROVED WITH WARNING")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS extraction_runs (
    run_id                TEXT NOT NULL,
    run_key               TEXT NOT NULL,
    pipeline_name         TEXT NOT NULL,
    business_date         TEXT NOT NULL,
    table_name            TEXT NOT NULL,
    extraction_started_at TEXT,
    extraction_ended_at   TEXT,
    rows_read             INTEGER,
    rows_extracted        INTEGER,
    rows_rejected         INTEGER,
    decision              TEXT,
    validation_results    TEXT,
    created_at            TEXT NOT NULL,
    PRIMARY KEY (run_id, table_name)
);
CREATE INDEX IF NOT EXISTS idx_runs_lookup
    ON extraction_runs (pipeline_name, business_date, table_name, decision);
"""


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime | None) -> str | None:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if dt else None


@dataclass
class ExtractionMetadata:
    """Audit record for a single staging table extraction (Section 4)."""

    run_id: str
    run_key: str
    pipeline_name: str
    business_date: str
    table_name: str
    extraction_started_at: str | None
    extraction_ended_at: str | None
    rows_read: int
    rows_extracted: int
    rows_rejected: int
    validation_results: dict[str, str]
    decision: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class MetadataStore:
    """SQLite-backed metadata log table plus local JSON log writer."""

    def __init__(self, db_path: Path, logs_dir: Path) -> None:
        self.db_path = Path(db_path)
        self.logs_dir = Path(logs_dir)
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

    # -- writes ---------------------------------------------------------------

    def record(self, metadata: ExtractionMetadata) -> None:
        """Persist an audit row and write the matching local JSON log file."""

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO extraction_runs (
                    run_id, run_key, pipeline_name, business_date, table_name,
                    extraction_started_at, extraction_ended_at, rows_read,
                    rows_extracted, rows_rejected, decision, validation_results,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (run_id, table_name) DO UPDATE SET
                    run_key               = excluded.run_key,
                    extraction_started_at = excluded.extraction_started_at,
                    extraction_ended_at   = excluded.extraction_ended_at,
                    rows_read             = excluded.rows_read,
                    rows_extracted        = excluded.rows_extracted,
                    rows_rejected         = excluded.rows_rejected,
                    decision              = excluded.decision,
                    validation_results    = excluded.validation_results
                """,
                (
                    metadata.run_id,
                    metadata.run_key,
                    metadata.pipeline_name,
                    metadata.business_date,
                    metadata.table_name,
                    metadata.extraction_started_at,
                    metadata.extraction_ended_at,
                    metadata.rows_read,
                    metadata.rows_extracted,
                    metadata.rows_rejected,
                    metadata.decision,
                    json.dumps(metadata.validation_results),
                    _iso(utcnow()),
                ),
            )
        self._write_json_log(metadata)

    def _write_json_log(self, metadata: ExtractionMetadata) -> None:
        day_dir = self.logs_dir / metadata.business_date
        day_dir.mkdir(parents=True, exist_ok=True)
        safe_table = metadata.table_name.replace(".", "_")
        log_file = day_dir / f"{metadata.run_id}_{safe_table}.json"
        log_file.write_text(
            json.dumps(metadata.to_dict(), indent=2, default=str),
            encoding="utf-8",
        )

    # -- reads ----------------------------------------------------------------

    def has_successful_run(
        self, pipeline_name: str, business_date: date | str, table_name: str
    ) -> bool:
        """V3 helper: does a previously successful run exist for this window?"""

        placeholders = ", ".join("?" for _ in SUCCESSFUL_DECISIONS)
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT 1 FROM extraction_runs
                WHERE pipeline_name = ? AND business_date = ? AND table_name = ?
                  AND decision IN ({placeholders})
                LIMIT 1
                """,
                (pipeline_name, str(business_date), table_name, *SUCCESSFUL_DECISIONS),
            ).fetchone()
        return row is not None

    def get_run(self, run_id: str, table_name: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM extraction_runs WHERE run_id = ? AND table_name = ?",
                (run_id, table_name),
            ).fetchone()
        return dict(row) if row else None

    # -- retention ------------------------------------------------------------

    def purge_expired(self, log_retention_days: int = 90, audit_retention_months: int = 24) -> dict[str, int]:
        """Section 5 - drop operational logs and audit rows past their retention."""

        removed_rows = 0
        with self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM extraction_runs WHERE created_at < datetime('now', ?)",
                (f"-{audit_retention_months} months",),
            )
            removed_rows = cur.rowcount

        removed_files = 0
        cutoff = datetime.now(timezone.utc).timestamp() - log_retention_days * 86400
        if self.logs_dir.exists():
            for child in self.logs_dir.iterdir():
                if child.is_dir() and child.stat().st_mtime < cutoff:
                    for f in child.glob("*.json"):
                        f.unlink(missing_ok=True)
                        removed_files += 1
        return {"audit_rows_removed": removed_rows, "log_files_removed": removed_files}
