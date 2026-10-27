"""Export validated staging Parquet partitions to Excel workbooks.

Usage:
    python scripts/export_excel.py --business-date 2026-10-06
    python scripts/export_excel.py --business-date 2026-10-06 --tables raw_logs
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
STAGING_DIR = PROJECT_ROOT / "data" / "staging"
EXPORT_DIR = PROJECT_ROOT / "data" / "exports"

TABLES = ("raw_logs", "raw_entities", "hb_pitc")
EXCEL_MAX_ROWS = 1_048_576


def _excel_safe(df: pl.DataFrame) -> pl.DataFrame:
    """Drop timezone info; Excel rejects tz-aware datetimes."""
    conversions = [
        pl.col(name).dt.replace_time_zone(None).alias(name)
        for name, dtype in df.schema.items()
        if isinstance(dtype, pl.Datetime) and dtype.time_zone
    ]
    return df.with_columns(conversions) if conversions else df


def export_table(table: str, business_date: date, staging_dir: Path, out_dir: Path) -> Path:
    partition = staging_dir / table / f"business_date={business_date.isoformat()}"
    parquet_file = partition / "data.parquet"
    if not parquet_file.exists():
        raise FileNotFoundError(
            f"No staging file for {table} on {business_date}: {parquet_file}"
        )

    df = _excel_safe(pl.read_parquet(parquet_file))
    if df.height >= EXCEL_MAX_ROWS:
        raise ValueError(
            f"{table} has {df.height} rows, exceeding the Excel limit of "
            f"{EXCEL_MAX_ROWS - 1} data rows. Use Parquet for large volumes."
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{table}_{business_date.isoformat()}.xlsx"
    df.write_excel(
        out_file,
        worksheet=table,
        autofit=True,
        freeze_panes=(1, 0),
    )
    return out_file


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export staging Parquet to Excel")
    parser.add_argument(
        "--business-date",
        default=date.today().isoformat(),
        help="Business date to export (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        choices=list(TABLES),
        default=list(TABLES),
        help="Subset of staging tables to export. Defaults to all.",
    )
    parser.add_argument("--staging-dir", type=Path, default=STAGING_DIR)
    parser.add_argument("--out-dir", type=Path, default=EXPORT_DIR)
    args = parser.parse_args(argv)

    bdate = date.fromisoformat(args.business_date)
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
