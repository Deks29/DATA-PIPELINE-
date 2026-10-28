"""Analytics fact tables for the Stage 2 transformed contract."""

from __future__ import annotations

from datetime import datetime

import polars as pl

from transformation.constants import (
    NULL_SENTINEL_INT,
    NULL_SENTINEL_STR,
    OUTPUT_COLUMNS,
    empty_frame,
    stable_key,
)


class FactMixin:
    """Build the three contracted analytics fact tables."""

    def _build_fact_prescribing(
        self,
        hb: pl.DataFrame,
        dim_date: pl.DataFrame,
        dim_hb: pl.DataFrame,
        dim_drug: pl.DataFrame,
        dim_type: pl.DataFrame,
        run_id: str,
        run_at: datetime,
    ) -> pl.DataFrame:
        if hb.height == 0:
            return empty_frame("fact_prescribing_monthly")
        dim_drug_keys = dim_drug.with_columns(
            pl.col("dmd_code").fill_null(NULL_SENTINEL_INT).alias("_dmd_key"),
            pl.col("bnf_item_code").fill_null(NULL_SENTINEL_STR).alias("_bnf_key"),
        ).select("_dmd_key", "_bnf_key", "drug_key")
        frame = (
            hb.join(
                dim_hb.select("health_board_code", "health_board_key"),
                left_on="HBT", right_on="health_board_code", how="left",
            )
            .join(dim_drug_keys, on=["_dmd_key", "_bnf_key"], how="left")
            .join(
                dim_type.select("prescribed_type_code", "prescribed_type_key"),
                left_on="PrescribedType", right_on="prescribed_type_code", how="left",
            )
            .join(
                dim_date.select(pl.col("calendar_date"), pl.col("date_key")),
                left_on="paid_month", right_on="calendar_date", how="left",
            )
        )
        frame = frame.with_columns(
            pl.col("health_board_key").fill_null(0),
            pl.col("drug_key").fill_null(0),
            pl.col("prescribed_type_key").fill_null(0),
            pl.col("date_key").fill_null(
                pl.col("paid_month").dt.year().cast(pl.Int32) * 10000
                + pl.col("paid_month").dt.month().cast(pl.Int32) * 100
                + pl.col("paid_month").dt.day().cast(pl.Int32)
            ).alias("period_key"),
        )
        grouped = frame.group_by(
            ["health_board_key", "drug_key", "prescribed_type_key", "period_key", "paid_month"]
        ).agg(
            pl.col("NumberOfPaidItems").sum().cast(pl.Int64).alias("paid_items"),
            pl.col("PaidQuantity").sum().round(4).alias("paid_quantity"),
            pl.col("GrossIngredientCost").sum().round(2).alias("gross_ingredient_cost"),
        )
        return grouped.with_columns(
            pl.struct(
                ["health_board_key", "drug_key", "prescribed_type_key", "period_key"]
            ).map_elements(
                lambda row: stable_key(
                    "fact_prescribing",
                    row["health_board_key"], row["drug_key"],
                    row["prescribed_type_key"], row["period_key"],
                ),
                return_dtype=pl.Int64,
            ).alias("prescribing_key"),
            pl.when(pl.col("paid_items") > 0)
            .then((pl.col("gross_ingredient_cost") / pl.col("paid_items")).round(4))
            .otherwise(None)
            .alias("avg_cost_per_item"),
            pl.when(pl.col("paid_quantity") > 0)
            .then((pl.col("gross_ingredient_cost") / pl.col("paid_quantity")).round(4))
            .otherwise(None)
            .alias("avg_cost_per_quantity"),
            (
                (pl.col("paid_quantity") == 0) & (pl.col("paid_items") > 0)
            ).alias("zero_quantity_flag"),
            pl.lit(run_id).alias("transformed_run_id"),
            pl.lit(run_at).cast(pl.Datetime("us", "UTC")).alias("updated_at"),
        ).select(OUTPUT_COLUMNS["fact_prescribing_monthly"])

    def _build_fact_event(
        self,
        logs: pl.DataFrame,
        dim_date: pl.DataFrame,
        dim_user: pl.DataFrame,
        run_id: str,
    ) -> pl.DataFrame:
        if logs.height == 0:
            return empty_frame("fact_event_daily")
        frame = (
            logs.group_by(["event_date", "user_id", "event_type"])
            .agg(pl.len().cast(pl.Int64).alias("event_count"))
            .join(
                dim_date.select(pl.col("calendar_date"), pl.col("date_key")),
                left_on="event_date", right_on="calendar_date", how="left",
            )
            .join(
                dim_user.select("user_id", "user_key"),
                on="user_id", how="left",
            )
            .with_columns(
                pl.col("date_key").alias("event_date_key"),
                pl.col("user_key").fill_null(0),
            )
        )
        return frame.with_columns(
            pl.struct(["event_date_key", "user_key", "event_type"]).map_elements(
                lambda row: stable_key(
                    "fact_event", row["event_date_key"], row["user_key"], row["event_type"]
                ),
                return_dtype=pl.Int64,
            ).alias("event_fact_key"),
            pl.lit(run_id).alias("transformed_run_id"),
        ).select(OUTPUT_COLUMNS["fact_event_daily"])

    def _build_fact_daily_summary(
        self,
        logs: pl.DataFrame,
        entities: pl.DataFrame,
        fact_event: pl.DataFrame,
        dim_date: pl.DataFrame,
        run_id: str,
    ) -> pl.DataFrame:
        event_summary = (
            fact_event.group_by("event_date_key")
            .agg(
                pl.col("event_count").sum().alias("total_events"),
                pl.col("user_key").n_unique().alias("unique_users"),
                pl.when(pl.col("event_type") == "signup").then(pl.col("event_count")).otherwise(0).sum().alias("signup_count"),
                pl.when(pl.col("event_type") == "purchase").then(pl.col("event_count")).otherwise(0).sum().alias("purchase_count"),
                pl.when(pl.col("event_type") == "view").then(pl.col("event_count")).otherwise(0).sum().alias("view_count"),
                pl.when(pl.col("event_type") == "click").then(pl.col("event_count")).otherwise(0).sum().alias("click_count"),
                pl.when(pl.col("event_type") == "logout").then(pl.col("event_count")).otherwise(0).sum().alias("logout_count"),
            )
        ) if fact_event.height else pl.DataFrame(
            schema={
                "event_date_key": pl.Int64,
                "total_events": pl.Int64,
                "unique_users": pl.UInt32,
                "signup_count": pl.Int64,
                "purchase_count": pl.Int64,
                "view_count": pl.Int64,
                "click_count": pl.Int64,
                "logout_count": pl.Int64,
            }
        )

        if entities.height:
            entity_summary = entities.group_by("created_date").agg(
                pl.len().cast(pl.Int64).alias("entities_created"),
                pl.when(pl.col("status") == "ACTIVE").then(1).otherwise(0).sum().alias("entities_active"),
                pl.when(pl.col("status") == "COMPLETED").then(1).otherwise(0).sum().alias("entities_completed"),
                pl.when(pl.col("status") == "CANCELLED").then(1).otherwise(0).sum().alias("entities_cancelled"),
            ).with_columns(
                (
                    pl.col("created_date").dt.year().cast(pl.Int32) * 10000
                    + pl.col("created_date").dt.month().cast(pl.Int32) * 100
                    + pl.col("created_date").dt.day().cast(pl.Int32)
                ).cast(pl.Int64).alias("event_date_key")
            ).drop("created_date")
        else:
            entity_summary = pl.DataFrame(
                schema={
                    "event_date_key": pl.Int64,
                    "entities_created": pl.Int64,
                    "entities_active": pl.Int64,
                    "entities_completed": pl.Int64,
                    "entities_cancelled": pl.Int64,
                }
            )

        if event_summary.height and entity_summary.height:
            summary = event_summary.join(entity_summary, on="event_date_key", how="full", coalesce=True)
        elif event_summary.height:
            summary = event_summary
        elif entity_summary.height:
            summary = entity_summary
        else:
            return empty_frame("fact_daily_summary")

        count_columns = [
            "total_events", "unique_users", "signup_count", "purchase_count",
            "view_count", "click_count", "logout_count", "entities_created",
            "entities_active", "entities_completed", "entities_cancelled",
        ]
        for column in count_columns:
            if column not in summary.columns:
                summary = summary.with_columns(pl.lit(0).cast(pl.Int64).alias(column))
        return summary.with_columns(
            pl.struct(["event_date_key"]).map_elements(
                lambda row: stable_key("fact_daily_summary", row["event_date_key"]),
                return_dtype=pl.Int64,
            ).alias("summary_key"),
            *[pl.col(c).cast(pl.Int64).fill_null(0).alias(c) for c in count_columns],
            pl.lit(run_id).alias("transformed_run_id"),
        ).select(OUTPUT_COLUMNS["fact_daily_summary"])
