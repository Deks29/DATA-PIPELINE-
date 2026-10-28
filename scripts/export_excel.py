"""Export validated staging Parquet partitions to Excel workbooks.

Usage:
    python scripts/export_excel.py --business-date 2026-10-06
    python scripts/export_excel.py --business-date 2026-10-06 --tables raw_logs
    python scripts/export_excel.py --source-file "C:/Users/me/Downloads/hb_pitc2026_07.csv"
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from extraction.cli import run_source_file
from extraction.config import TABLES
from extraction.excel import export_table

STAGING_DIR = PROJECT_ROOT / "data" / "staging"
EXPORT_DIR = PROJECT_ROOT / "data" / "exports"

EXPORT_TABLES = ("raw_logs", "raw_entities", "hb_pitc")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export staging Parquet to Excel")
    parser.add_argument(
        "--business-date",
        default=None,
        help="Business date to export (YYYY-MM-DD). Inferred with --source-file.",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        choices=list(EXPORT_TABLES),
        default=list(EXPORT_TABLES),
        help="Subset of staging tables to export. Defaults to all.",
    )
    parser.add_argument(
        "--source-file",
        type=Path,
        help="Optional raw CSV/JSON to extract first, then export to Excel.",
    )
    parser.add_argument("--staging-dir", type=Path, default=STAGING_DIR)
    parser.add_argument("--out-dir", type=Path, default=EXPORT_DIR)
    args = parser.parse_args(argv)

    if args.source_file:
        try:
            results, bdate = run_source_file(
                args.source_file,
                business_date=args.business_date,
                tables=args.tables,
                force=True,
            )
        except (FileNotFoundError, ValueError) as exc:
            print(f"[FAIL] {exc}", file=sys.stderr)
            return 1
        failed = [r for r in results if not r.is_success]
        if failed:
            for result in failed:
                print(f"[FAIL] {result.table_name}: {result.message}", file=sys.stderr)
            return 1
    else:
        bdate = date.fromisoformat(args.business_date) if args.business_date else date.today()

    failed = False
    for table in args.tables:
        try:
            out_file = export_table(table, bdate, args.staging_dir, args.out_dir)
            print(f"[OK] {table} -> {out_file}")
        except (FileNotFoundError, ValueError) as exc:
            failed = True
            print(f"[FAIL] {table}: {exc}", file=sys.stderr)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
