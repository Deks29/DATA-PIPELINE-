"""Analytics dimension tables for the Stage 2 transformed contract."""

from __future__ import annotations

from datetime import date, datetime

import polars as pl

from transformation.constants import (
    EMPTY_SCHEMAS,
    KNOWN_PRESCRIBED_TYPES,
    OUTPUT_COLUMNS,
    UNKNOWN_CODE,
    UNKNOWN_USER,
    empty_frame,
    stable_key,
)


class DimensionMixin:
    """Build the six contracted analytics dimension tables."""

    def _build_dim_date(self, dates: list[date], bdate: date) -> pl.DataFrame:
        unique = sorted({d for d in dates if d is not None}) or [bdate]
        frame = pl.DataFrame({"calendar_date": unique}).with_columns(
            pl.col("calendar_date").dt.year().cast(pl.Int32).alias("year"),
            pl.col("calendar_date").dt.quarter().cast(pl.Int32).alias("quarter"),
            pl.col("calendar_date").dt.month().cast(pl.Int32).alias("month"),
            pl.col("calendar_date").dt.day().cast(pl.Int32).alias("day"),
            pl.col("calendar_date").dt.week().cast(pl.Int32).alias("week_of_year"),
            pl.col("calendar_date").dt.weekday().is_in([6, 7]).alias("is_weekend"),
        ).with_columns(
            (
                pl.col("year") * 10000 + pl.col("month") * 100 + pl.col("day")
            ).cast(pl.Int32).alias("date_key")
        )
        return frame.select(OUTPUT_COLUMNS["dim_date"])

    def _build_dim_user(
        self, logs: pl.DataFrame, entities: pl.DataFrame, run_at: datetime
    ) -> pl.DataFrame:
        frames: list[pl.DataFrame] = []
        if logs.height:
            frames.append(logs.select(pl.col("user_id"), pl.col("timestamp").alias("seen_at")))
        if entities.height:
            frames.append(entities.select(pl.col("user_id"), pl.col("created_at").alias("seen_at")))
        if not frames:
            base = pl.DataFrame(schema={"user_id": pl.String, "first_seen_at": pl.Datetime("us", "UTC"), "last_seen_at": pl.Datetime("us", "UTC")})
        else:
            base = pl.concat(frames, how="diagonal_relaxed").group_by("user_id").agg(
                pl.col("seen_at").min().alias("first_seen_at"),
                pl.col("seen_at").max().alias("last_seen_at"),
            )
        return base.with_columns(
            pl.struct(["user_id"]).map_elements(
                lambda row: stable_key("dim_user", row["user_id"]), return_dtype=pl.Int64
            ).alias("user_key"),
            (pl.col("user_id") == UNKNOWN_USER).alias("user_is_unknown"),
            pl.lit(run_at).cast(pl.Datetime("us", "UTC")).alias("updated_at"),
        ).select(OUTPUT_COLUMNS["dim_user"])

    def _build_dim_health_board(self, hb: pl.DataFrame, run_at: datetime) -> pl.DataFrame:
        codes = sorted(set(hb["HBT"].to_list())) if hb.height else []
        frame = pl.DataFrame({"health_board_code": codes})
        return frame.with_columns(
            pl.struct(["health_board_code"]).map_elements(
                lambda row: stable_key("dim_health_board", row["health_board_code"]),
                return_dtype=pl.Int64,
            ).alias("health_board_key"),
            pl.col("health_board_code").alias("health_board_name"),
            (pl.col("health_board_code") == UNKNOWN_CODE).alias("is_unknown"),
        ).select(OUTPUT_COLUMNS["dim_health_board"])

    def _build_dim_drug(self, hb: pl.DataFrame, run_at: datetime) -> pl.DataFrame:
        if hb.height:
            frame = (
                hb.select(
                    pl.col("DMDCode").alias("dmd_code"),
                    pl.col("BNFItemCode").alias("bnf_item_code"),
                    pl.col("BNFItemDescription").alias("bnf_item_description"),
                )
                .unique(subset=["dmd_code", "bnf_item_code"], keep="last", maintain_order=True)
            )
        else:
            frame = pl.DataFrame(
                schema={
                    "dmd_code": pl.Int64,
                    "bnf_item_code": pl.String,
                    "bnf_item_description": pl.String,
                }
            )
        return frame.with_columns(
            pl.struct(["dmd_code", "bnf_item_code"]).map_elements(
                lambda row: stable_key("dim_drug", row["dmd_code"], row["bnf_item_code"]),
                return_dtype=pl.Int64,
            ).alias("drug_key"),
            pl.col("bnf_item_description").fill_null("Aggregate prescribing row"),
            (
                pl.col("dmd_code").is_null() & pl.col("bnf_item_code").is_null()
            ).alias("is_aggregate"),
            pl.lit(run_at).cast(pl.Datetime("us", "UTC")).alias("updated_at"),
        ).select(OUTPUT_COLUMNS["dim_drug"])

    def _build_dim_prescribed_type(self, hb: pl.DataFrame) -> pl.DataFrame:
        codes = sorted({c for c in hb["PrescribedType"].to_list()} | set(KNOWN_PRESCRIBED_TYPES)) if hb.height else list(KNOWN_PRESCRIBED_TYPES)
        rows = [
            {
                "prescribed_type_key": KNOWN_PRESCRIBED_TYPES.get(code, (0, "Unknown prescribed type"))[0],
                "prescribed_type_code": code,
                "prescribed_type_description": KNOWN_PRESCRIBED_TYPES.get(code, (0, "Unknown prescribed type"))[1],
            }
            for code in codes
        ]
        return pl.DataFrame(rows).select(OUTPUT_COLUMNS["dim_prescribed_type"])

    def _build_dim_device(self, device: pl.DataFrame, run_at: datetime) -> pl.DataFrame:
        if device.height == 0:
            return empty_frame("dim_device_classification")
        frame = device.with_columns(
            pl.struct(["product_code"]).map_elements(
                lambda row: stable_key("dim_device", row["product_code"]),
                return_dtype=pl.Int64,
            ).alias("device_key"),
            pl.col("last_updated_date").alias("last_updated"),
            pl.lit(run_at).cast(pl.Datetime("us", "UTC")).alias("updated_at"),
        )
        text_columns = [
            "device_name", "regulation_number", "medical_specialty_description",
            "review_panel", "submission_type_id", "definition",
        ]
        for column in text_columns:
            if column not in frame.columns:
                frame = frame.with_columns(pl.lit(None, dtype=pl.String).alias(column))
            else:
                frame = frame.with_columns(pl.col(column).cast(pl.String))
        for column in (
            "implant_flag", "life_sustain_support_flag", "gmp_exempt_flag", "third_party_flag"
        ):
            if column not in frame.columns:
                frame = frame.with_columns(pl.lit(None, dtype=pl.Boolean).alias(column))
        return frame.with_columns(
            pl.col("device_class").cast(pl.String),
            pl.col("last_updated").cast(pl.Date),
        ).select(OUTPUT_COLUMNS["dim_device_classification"])
