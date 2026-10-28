"""Fetch openFDA API records and save them as an Excel workbook.

Examples
--------
    # Default: up to 500 records, using the search query from .env
    python scripts/api_to_excel.py

    # Fetch a specific product code
    python scripts/api_to_excel.py --search "product_code:NOB"

    # Fetch the first 500 device classifications (no search filter)
    python scripts/api_to_excel.py --search "" --limit 500

    # Choose the output file
    python scripts/api_to_excel.py --limit 500 --output data/exports/device_classification.xlsx
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from extraction.api import fetch_records, normalise_records
from extraction.config import load_settings

EXCEL_MAX_ROWS = 1_048_576


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fetch openFDA API records and save them as an .xlsx workbook"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Maximum number of API records to fetch (default: 500).",
    )
    parser.add_argument(
        "--search",
        default=None,
        help="openFDA search query. Omit to use API_SEARCH from .env; "
        "pass an empty string to fetch all classifications.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output .xlsx path. Defaults to data/exports/openfda_device_classification_<date>.xlsx.",
    )
    args = parser.parse_args(argv)

    if args.limit < 1:
        parser.error("--limit must be at least 1")
    if args.limit >= EXCEL_MAX_ROWS:
        parser.error(f"--limit must be below the Excel row limit ({EXCEL_MAX_ROWS})")

    settings = load_settings()
    if not settings.api_url:
        print("Error: no API endpoint configured. Set API=... in .env.", file=sys.stderr)
        return 1

    search = args.search if args.search is not None else settings.api_search
    search = search or None  # empty string means "no filter"

    try:
        records, meta = fetch_records(
            settings.api_url,
            search=search,
            limit=min(args.limit, 1000),
            api_key=settings.api_key or None,
            max_records=args.limit,
        )
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if not records:
        print("No records returned by the API for this search.", file=sys.stderr)
        return 1

    fields = sorted({key for record in records for key in record})
    last_updated = str((meta or {}).get("last_updated") or date.today().isoformat())
    rows = normalise_records(records, fields, last_updated)
    df = pl.DataFrame(rows)

    out_file = args.output or (
        settings.base_dir
        / "data"
        / "exports"
        / f"openfda_device_classification_{date.today().isoformat()}.xlsx"
    )
    out_file = Path(out_file).expanduser()
    out_file.parent.mkdir(parents=True, exist_ok=True)

    df.write_excel(
        out_file,
        worksheet="device_classification",
        autofit=True,
        freeze_panes=(1, 0),
    )

    print(f"Fetched {len(records)} record(s) from {settings.api_url}")
    print(f"Search: {search or '(none)'}")
    print(f"Saved: {out_file}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
