"""Export validated staging Parquet partitions to Excel workbooks."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

EXCEL_MAX_ROWS = 1_048_576


def excel_safe(df: pl.DataFrame) -> pl.DataFrame:
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

    df = excel_safe(pl.read_parquet(parquet_file))
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
