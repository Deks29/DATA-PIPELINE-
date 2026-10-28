"""Run the Stage 2 transformation and export the analytics model to Excel.

Usage:
    python scripts/export_transformed_excel.py
    python scripts/export_transformed_excel.py --business-date 2026-07-01 --force
    python scripts/export_transformed_excel.py --no-run
    python scripts/export_transformed_excel.py --tables fact_event_daily fact_daily_summary
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import polars as pl
import xlsxwriter

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from extraction.excel import EXCEL_MAX_ROWS, excel_safe
from transformation.config import load_transform_settings
from transformation.pipeline import REJECTED, TransformationPipeline

DEFAULT_EXPORT_DIR = PROJECT_ROOT / "data" / "exports"


def _read_table(table_dir: Path) -> pl.DataFrame | None:
    files = sorted(table_dir.rglob("data.parquet")) if table_dir.exists() else []
    if not files:
        return None
    return pl.concat([pl.read_parquet(path) for path in files], how="vertical_relaxed")


def _table_keys(results) -> list[str]:
    return [result.table_name.split(".", 1)[1] for result in results]


def _discover_tables(transformed_dir: Path) -> list[str]:
    return sorted(
        child.name
        for child in transformed_dir.iterdir()
        if child.is_dir() and not child.name.startswith("_")
    ) if transformed_dir.exists() else []


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the Stage 2 transformation and export the transformed tables to Excel"
    )
    parser.add_argument("--business-date", help="Business date YYYY-MM-DD (defaults to latest partition)")
    parser.add_argument("--tables", nargs="+", help="Subset of transformed tables to export")
    parser.add_argument("--output", type=Path, help="Output .xlsx path")
    parser.add_argument(
        "--no-run",
        action="store_true",
        help="Export the existing transformed Parquet without running the transformation",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Rewrite existing transformed partitions when the transformation is run",
    )
    args = parser.parse_args(argv)

    settings = load_transform_settings()
    pipeline = TransformationPipeline(settings=settings)
    bdate = pipeline._resolve_business_date(args.business_date)

    results = []
    if not args.no_run:
        results = pipeline.run(bdate, force=args.force)
        decision = results[0].decision if results else REJECTED
        print(f"[{decision}] run_id={results[0].run_id if results else None} business_date={bdate}")
        if decision == REJECTED:
            print("Transformation was REJECTED; no transformed output to export.", file=sys.stderr)
            return 1
        table_keys = _table_keys(results)
    else:
        table_keys = _discover_tables(Path(settings.transformed_dir))

    if args.tables:
        requested = set(args.tables)
        unknown = requested - set(table_keys)
        if unknown:
            print(f"Unknown transformed table(s): {sorted(unknown)}", file=sys.stderr)
            return 1
        table_keys = [key for key in table_keys if key in requested]

    frames: dict[str, pl.DataFrame] = {}
    for key in table_keys:
        frame = _read_table(settings.transformed_dir_for(key))
        if frame is not None:
            frames[key] = frame

    if not frames:
        print("No transformed Parquet files found to export.", file=sys.stderr)
        return 1

    output = args.output or (DEFAULT_EXPORT_DIR / f"transformed_analytics_{bdate.isoformat()}.xlsx")
    output = Path(output).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)

    workbook = xlsxwriter.Workbook(str(output))
    try:
        if results:
            summary = pl.DataFrame(
                {
                    "table_name": [r.table_name for r in results if r.table_name.split(".", 1)[1] in frames],
                    "decision": [r.decision for r in results if r.table_name.split(".", 1)[1] in frames],
                    "records_read": [r.records_read for r in results if r.table_name.split(".", 1)[1] in frames],
                    "records_output": [r.records_output for r in results if r.table_name.split(".", 1)[1] in frames],
                    "records_rejected": [r.records_rejected for r in results if r.table_name.split(".", 1)[1] in frames],
                    "run_id": [r.run_id for r in results if r.table_name.split(".", 1)[1] in frames],
                }
            )
            summary.write_excel(
                workbook=workbook,
                worksheet="run_summary",
                table_name="tbl_run_summary",
                autofit=True,
                freeze_panes=(1, 0),
            )

        for key, frame in frames.items():
            if frame.height >= EXCEL_MAX_ROWS:
                print(
                    f"Skipping {key}: {frame.height} rows exceeds the Excel limit "
                    f"of {EXCEL_MAX_ROWS - 1}.",
                    file=sys.stderr,
                )
                continue
            safe = excel_safe(frame)
            safe.write_excel(
                workbook=workbook,
                worksheet=key,
                table_name=f"tbl_{key}",
                autofit=True,
                freeze_panes=(1, 0),
            )
            print(f"  {key}: {frame.height} row(s)")
    finally:
        workbook.close()

    print(f"Saved: {output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
