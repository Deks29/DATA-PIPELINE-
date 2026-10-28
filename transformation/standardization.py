"""Standardization and data-type rules S-01..S-10 for Stage 2."""

from __future__ import annotations

import polars as pl

from transformation.constants import KNOWN_PRESCRIBED_TYPES

_DEVICE_FLAGS = (
    "implant_flag",
    "life_sustain_support_flag",
    "gmp_exempt_flag",
    "third_party_flag",
)

_DEVICE_CLASS_MAP = {
    "1": "CLASS_1",
    "2": "CLASS_2",
    "3": "CLASS_3",
    "U": "UNCLASSIFIED",
    "N": "NOT_CLASSIFIED",
}


def require_columns(df: pl.DataFrame, columns: list[str], table: str) -> None:
    """Fail the batch when a required staging column is missing (schema drift)."""
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"{table} is missing required staging column(s): {missing}")


def standardise_log_columns(frame: pl.DataFrame) -> pl.DataFrame:
    """S-02/S-10: lowercase event types and trim log/user identifiers."""
    return frame.with_columns(
        pl.col("log_id").cast(pl.String).str.strip_chars().alias("log_id"),
        pl.col("event_type").cast(pl.String).alias("_original_event_type"),
        pl.col("user_id").cast(pl.String).alias("_original_user_id"),
    ).with_columns(
        pl.col("_original_event_type").str.strip_chars().str.to_lowercase().fill_null("unknown").alias("event_type"),
        pl.col("_original_user_id").str.strip_chars().alias("user_id"),
    )


def standardise_entity_columns(frame: pl.DataFrame) -> pl.DataFrame:
    """S-02/S-10: uppercase statuses and trim entity/user identifiers."""
    return frame.with_columns(
        pl.col("entity_id").cast(pl.String).str.strip_chars().alias("entity_id"),
        pl.col("user_id").cast(pl.String).str.strip_chars().alias("user_id"),
        pl.col("status").cast(pl.String).str.strip_chars().str.to_uppercase().fill_null("UNKNOWN").alias("status"),
    )


def standardise_hb_columns(frame: pl.DataFrame, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    """S-01..S-06: normalise PITC codes, text, measures and paid month."""
    frame = frame.with_columns(
        pl.col("HBT").cast(pl.String).str.strip_chars().str.to_uppercase().alias("HBT"),
        pl.col("PrescribedType").cast(pl.String).str.strip_chars().str.to_uppercase().alias("PrescribedType"),
        pl.col("BNFItemCode").cast(pl.String).str.strip_chars().alias("BNFItemCode"),
        pl.col("BNFItemDescription").cast(pl.String).str.strip_chars().str.replace_all(r"\s+", " ").alias("BNFItemDescription"),
        pl.col("DMDCode").cast(pl.Int64, strict=False).alias("DMDCode"),
        pl.col("NumberOfPaidItems").cast(pl.Int64, strict=False).alias("NumberOfPaidItems"),
        pl.col("PaidQuantity").cast(pl.Float64, strict=False).alias("PaidQuantity"),
        pl.col("GrossIngredientCost").cast(pl.Float64, strict=False).alias("GrossIngredientCost"),
    )
    if schema.get("PaidDateMonth") == pl.String:
        frame = frame.with_columns(
            pl.col("PaidDateMonth").str.to_datetime("%Y%m", strict=False).alias("PaidDateMonth")
        )
    return frame


def standardise_device_codes(frame: pl.DataFrame) -> pl.DataFrame:
    """S-03: uppercase device codes without losing leading zeros."""
    return frame.with_columns(
        pl.col("product_code").cast(pl.String).str.strip_chars().str.to_uppercase().alias("product_code"),
        pl.col("device_class").cast(pl.String).str.strip_chars().str.to_uppercase().alias("device_class"),
    )


def standardise_device_attributes(frame: pl.DataFrame) -> pl.DataFrame:
    """S-01/S-07/S-09: Y/N flags to Boolean and device class labels."""
    flag_exprs = [
        pl.when(pl.col(column).cast(pl.String).str.to_uppercase() == "Y")
        .then(pl.lit(True))
        .when(pl.col(column).cast(pl.String).str.to_uppercase() == "N")
        .then(pl.lit(False))
        .otherwise(pl.lit(None, dtype=pl.Boolean))
        .alias(column)
        for column in _DEVICE_FLAGS
    ]
    return frame.with_columns(
        pl.col("product_code").cast(pl.String),
        *flag_exprs,
    ).with_columns(
        pl.col("device_class").replace_strict(
            _DEVICE_CLASS_MAP, default=pl.col("device_class")
        ).alias("device_class"),
        pl.col("last_updated").cast(pl.Datetime("us")).dt.date().alias("last_updated_date"),
    )
