"""Command line interface for the SightSync Stage 1 extraction engine.

Examples
--------
    python -m extraction.cli --business-date 2026-10-06
    python -m extraction.cli --business-date 2026-10-06 --tables raw_logs --force
    python -m extraction.cli --business-date 2026-10-06 --purge-retention
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

from extraction.config import TABLES, load_settings
from extraction.metadata import MetadataStore
from extraction.pipeline import (
    APPROVED,
    APPROVED_WITH_WARNING,
    MANUAL_REVIEW,
    ExtractionPipeline,
    to_json,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="extraction",
        description="SightSync Batch Pipeline - Stage 1 Data Extraction (Polars)",
    )
    parser.add_argument(
        "--business-date",
        required=False,
        default=date.today().isoformat(),
        help="Business date to extract (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        choices=list(TABLES),
        default=list(TABLES),
        help="Subset of staging tables to extract. Defaults to all.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-extract even if a prior successful run exists (atomic overwrite).",
    )
    parser.add_argument(
        "--purge-retention",
        action="store_true",
        help="Purge logs/audit rows past their retention window, then exit.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON summary.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    settings = load_settings()
    store = MetadataStore(settings.metadata_db, settings.logs_dir)

    if args.purge_retention:
        removed = store.purge_expired(
            settings.operational_log_retention_days, settings.audit_retention_months
        )
        print(f"Retention purge complete: {removed}")
        return 0

    pipeline = ExtractionPipeline(settings=settings, store=store)
    results = pipeline.run(args.business_date, tables=args.tables, force=args.force)

    if args.json:
        print(to_json(results))
    else:
        for r in results:
            symbols = " ".join(f"{cid}={st}" for cid, st in r.check_map.items()) or "-"
            print(f"[{r.decision}] {r.table_name} ({args.business_date})")
            print(
                f"    rows_read={r.rows_read} rows_extracted={r.rows_extracted} "
                f"rows_rejected={r.rows_rejected}"
            )
            print(f"    checks: {symbols}")
            if r.parquet_path:
                print(f"    staging: {r.parquet_path}")
            if r.message:
                print(f"    message: {r.message}")

    # Airflow consumes the exit code to drive retries / engineer alerts.
    if any(r.decision == MANUAL_REVIEW for r in results):
        return 2
    if all(r.decision in (APPROVED, APPROVED_WITH_WARNING) for r in results):
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
