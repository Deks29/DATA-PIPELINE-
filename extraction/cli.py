"""Command line interface for the SightSync Stage 1 extraction engine.

Examples
--------
    python -m extraction.cli --business-date 2026-10-06
    python -m extraction.cli --business-date 2026-10-06 --tables raw_logs --force
    python -m extraction.cli --business-date 2026-10-06 --purge-retention
    python -m extraction.cli --source-file "C:/Users/me/Downloads/hb_pitc2026_07.csv"
"""

from __future__ import annotations

import argparse
import fnmatch
import re
import shutil
import sys
import tempfile
from dataclasses import replace
from datetime import date
from pathlib import Path

import polars as pl

from extraction.config import TABLES, load_settings
from extraction.metadata import MetadataStore
from extraction.pipeline import (
    APPROVED,
    APPROVED_WITH_WARNING,
    MANUAL_REVIEW,
    ExtractionPipeline,
    to_json,
)

_DATE_RE = re.compile(r"(20\d{2})[-_]?(0[1-9]|1[0-2])[-_]?(0[1-9]|[12]\d|3[01])")
_MONTH_RE = re.compile(r"(20\d{2})[-_]?(0[1-9]|1[0-2])")


def _resolve_source_table(path: Path, requested: list[str]) -> str:
    name = path.name.lower()
    for table_key in TABLES:
        if name.startswith(table_key):
            return table_key
    if len(requested) == 1:
        return requested[0]
    raise ValueError(
        f"Cannot infer the table for '{path.name}'. "
        f"Pass --tables with one of {list(TABLES)}."
    )


def _infer_business_date(source: Path, table_key: str) -> date:
    if table_key == "hb_pitc":
        month = None
        if source.suffix.lower() == ".csv":
            try:
                month = (
                    pl.scan_csv(source, n_rows=1000)
                    .select(pl.col("PaidDateMonth").drop_nulls().first())
                    .collect()
                    .item()
                )
            except Exception:
                month = None
        if month:
            return date(int(month) // 100, int(month) % 100, 1)
        match = _MONTH_RE.search(source.stem)
        if match:
            return date(int(match.group(1)), int(match.group(2)), 1)

    match = _DATE_RE.search(source.stem)
    if match:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    return date.today()


def _stage_source_file(
    source: Path, table_key: str, business_date: date, raw_root: Path
) -> Path:
    suffix = source.suffix.lower()
    if suffix not in (".csv", ".json"):
        raise ValueError(
            f"Unsupported source extension '{source.suffix}'. Use CSV or JSON."
        )

    dest_dir = raw_root / table_key
    dest_dir.mkdir(parents=True, exist_ok=True)
    pattern = TABLES[table_key].file_glob.format(ext=suffix.lstrip("."))
    if fnmatch.fnmatch(source.name.lower(), pattern.lower()):
        dest = dest_dir / source.name
    else:
        dest = dest_dir / f"{table_key}_{business_date.isoformat()}{suffix}"

    if dest.resolve() != source.resolve():
        shutil.copy2(source, dest)
    return dest


def run_source_file(
    source_file: str | Path,
    business_date: str | None = None,
    tables: list[str] | None = None,
    force: bool = False,
    settings=None,
    store: MetadataStore | None = None,
) -> tuple[list, date]:
    """Stage an arbitrary CSV/JSON file, extract it, and return the results."""

    source = Path(source_file).expanduser()
    if not source.is_file():
        raise FileNotFoundError(f"Source file not found: {source}")

    settings = settings or load_settings()
    table_key = _resolve_source_table(source, tables or list(TABLES))
    bdate = (
        date.fromisoformat(business_date)
        if business_date
        else _infer_business_date(source, table_key)
    )
    store = store or MetadataStore(settings.metadata_db, settings.logs_dir)

    with tempfile.TemporaryDirectory(prefix="sightsync_source_") as tmp:
        raw_root = Path(tmp)
        _stage_source_file(source, table_key, bdate, raw_root)
        run_settings = replace(settings, raw_dir=raw_root)
        pipeline = ExtractionPipeline(settings=run_settings, store=store)
        results = pipeline.run(bdate, tables=[table_key], force=force)
    return results, bdate


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="extraction",
        description="SightSync Batch Pipeline - Stage 1 Data Extraction (Polars)",
    )
    parser.add_argument(
        "--business-date",
        required=False,
        default=None,
        help="Business date to extract (YYYY-MM-DD). Defaults to today, or is "
        "inferred from --source-file.",
    )
    parser.add_argument(
        "--source-file",
        type=Path,
        help="Extract a specific CSV/JSON file (e.g. from Downloads) instead of "
        "the default raw directory.",
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
        "--excel",
        action="store_true",
        help="Also write Excel workbooks to data/exports (implied by --source-file).",
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

    if args.source_file:
        try:
            results, bdate = run_source_file(
                args.source_file,
                business_date=args.business_date,
                tables=args.tables,
                force=args.force,
                settings=settings,
                store=store,
            )
        except (FileNotFoundError, ValueError) as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1
        business_date = bdate.isoformat()
        args.excel = True
    else:
        business_date = args.business_date or date.today().isoformat()
        pipeline = ExtractionPipeline(settings=settings, store=store)
        results = pipeline.run(business_date, tables=args.tables, force=args.force)

    if args.json:
        print(to_json(results))
    else:
        for r in results:
            symbols = " ".join(f"{cid}={st}" for cid, st in r.check_map.items()) or "-"
            print(f"[{r.decision}] {r.table_name} ({business_date})")
            print(
                f"    rows_read={r.rows_read} rows_extracted={r.rows_extracted} "
                f"rows_rejected={r.rows_rejected}"
            )
            print(f"    checks: {symbols}")
            if r.parquet_path:
                print(f"    staging: {r.parquet_path}")
            if r.message:
                print(f"    message: {r.message}")

    if args.excel:
        from extraction.excel import export_table

        out_dir = settings.base_dir / "data" / "exports"
        bdate = date.fromisoformat(business_date)
        for r in results:
            if r.is_success:
                try:
                    out_file = export_table(r.table_key, bdate, settings.staging_dir, out_dir)
                    print(f"    excel: {out_file}")
                except (FileNotFoundError, ValueError) as exc:
                    print(f"    excel error: {exc}", file=sys.stderr)

    # Airflow consumes the exit code to drive retries / engineer alerts.
    if any(r.decision == MANUAL_REVIEW for r in results):
        return 2
    if all(r.decision in (APPROVED, APPROVED_WITH_WARNING) for r in results):
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
