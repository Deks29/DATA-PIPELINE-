"""Data cleaning rules C-01..C-12 for Stage 2 source tables."""

from __future__ import annotations

import json
from datetime import date

import polars as pl

from transformation.constants import (
    HBT_PATTERN,
    KNOWN_PRESCRIBED_TYPES,
    NULL_SENTINEL_INT,
    NULL_SENTINEL_STR,
    UNKNOWN_CODE,
    UNKNOWN_USER,
)
from transformation.metadata import utcnow
from transformation.models import SourceState
from transformation import standardization


class CleaningMixin:
    """Clean, deduplicate and reject Stage 1 staging records."""

    @staticmethod
    def _require_columns(df: pl.DataFrame, columns: list[str], table: str) -> None:
        standardization.require_columns(df, columns, table)

    @staticmethod
    def _clean_text(frame: pl.DataFrame, columns: list[str], collapse: bool = False) -> pl.DataFrame:
        exprs = []
        for column in columns:
            expr = pl.col(column).cast(pl.String).str.strip_chars()
            if collapse:
                expr = expr.str.replace_all(r"\s+", " ")
            exprs.append(expr.alias(column))
        return frame.with_columns(exprs) if exprs else frame

    @staticmethod
    def _reject(
        state: SourceState,
        frame: pl.DataFrame,
        mask: pl.Expr,
        rule_id: str,
        reason: str,
        run_id: str,
        batch_id: str,
        bdate: date,
        parent_run_id: str | None,
    ) -> pl.DataFrame:
        mask = mask.fill_null(True)
        rejected = frame.filter(mask)
        accepted = frame.filter(~mask)
        if rejected.height:
            rejected = rejected.with_columns(
                pl.lit(rule_id).alias("rule_id"),
                pl.lit(reason).alias("rejection_reason"),
                pl.lit(state.name).alias("source_table"),
                pl.lit(bdate).cast(pl.Date).alias("business_date"),
                pl.lit(run_id).alias("run_id"),
                pl.lit(batch_id).alias("batch_id"),
                pl.lit(parent_run_id).alias("parent_run_id"),
                pl.lit(utcnow()).cast(pl.Datetime("us", "UTC")).alias("rejected_at"),
            )
            state.rejected_frames.append(rejected)
            state.records_rejected += rejected.height
            state.add_rule(rule_id, rejected.height)
        return accepted

    @staticmethod
    def _dedupe(
        state: SourceState,
        frame: pl.DataFrame,
        keys: list[str],
    ) -> pl.DataFrame:
        before = frame.height
        if "extracted_at" in frame.columns:
            frame = frame.sort(["extracted_at", "run_id"])
        frame = frame.unique(subset=keys, keep="last", maintain_order=True)
        removed = before - frame.height
        state.records_deduplicated += removed
        state.add_rule("C-01", removed)
        return frame

    def _parent_for(self, frame: pl.DataFrame) -> str | None:
        if frame.height and "run_id" in frame.columns:
            ordered = frame.sort("extracted_at") if "extracted_at" in frame.columns else frame
            return str(ordered["run_id"][-1])
        return None

    def _clean_logs(self, raw: pl.DataFrame, state: SourceState, run_id: str, batch_id: str, bdate: date) -> pl.DataFrame:
        self._require_columns(raw, ["log_id", "event_type", "user_id", "timestamp", "payload"], "raw_logs")
        state.records_read = raw.height
        state.parent_run_id = self._parent_for(raw)
        if raw.height == 0:
            state.records_output = 0
            return raw.with_columns(pl.lit(None, dtype=pl.Date).alias("event_date"))

        frame = standardization.standardise_log_columns(raw)
        frame = self._reject(
            state, frame, pl.col("log_id").is_null() | (pl.col("log_id") == ""),
            "C-02", "Null or blank primary key log_id", run_id, batch_id, bdate, state.parent_run_id,
        )

        missing_user = pl.col("user_id").is_null() | (pl.col("user_id") == "")
        unknown_count = frame.filter(missing_user).height
        frame = frame.with_columns(
            pl.when(missing_user).then(pl.lit(UNKNOWN_USER)).otherwise(pl.col("user_id")).alias("user_id")
        )
        state.add_rule("C-03", unknown_count)

        event_modified = frame.filter(
            pl.col("_original_event_type").is_null()
            | (pl.col("event_type") != pl.col("_original_event_type"))
        ).height

        frame = self._reject(
            state, frame, pl.col("timestamp").is_null(),
            "S-01", "Unparseable or missing event timestamp", run_id, batch_id, bdate, state.parent_run_id,
        )

        frame, invalid_payload = self._canonicalise_payload(frame, state)
        state.add_rule("C-05", invalid_payload)
        state.records_modified += event_modified + unknown_count + invalid_payload

        frame = frame.drop(["_original_event_type", "_original_user_id"])
        frame = self._dedupe(state, frame, ["log_id"])
        frame = frame.with_columns(pl.col("timestamp").dt.date().alias("event_date"))
        state.records_output = frame.height
        state.records_accepted = frame.height
        return frame

    def _clean_entities(self, raw: pl.DataFrame, state: SourceState, run_id: str, batch_id: str, bdate: date) -> pl.DataFrame:
        self._require_columns(raw, ["entity_id", "user_id", "status", "created_at"], "raw_entities")
        state.records_read = raw.height
        state.parent_run_id = self._parent_for(raw)
        if raw.height == 0:
            state.records_output = 0
            return raw.with_columns(pl.lit(None, dtype=pl.Date).alias("created_date"))

        frame = standardization.standardise_entity_columns(raw)
        frame = self._reject(
            state, frame, pl.col("entity_id").is_null() | (pl.col("entity_id") == ""),
            "C-02", "Null or blank primary key entity_id", run_id, batch_id, bdate, state.parent_run_id,
        )
        frame = self._reject(
            state, frame, pl.col("created_at").is_null(),
            "S-01", "Unparseable or missing created_at timestamp", run_id, batch_id, bdate, state.parent_run_id,
        )

        missing_user = pl.col("user_id").is_null() | (pl.col("user_id") == "")
        unknown_count = frame.filter(missing_user).height
        frame = frame.with_columns(
            pl.when(missing_user).then(pl.lit(UNKNOWN_USER)).otherwise(pl.col("user_id")).alias("user_id")
        )
        state.add_rule("C-03", unknown_count)

        known_status = pl.col("status").is_in(["ACTIVE", "COMPLETED", "CANCELLED", "PENDING"])
        unknown_status = frame.filter(~known_status).height
        frame = frame.with_columns(
            pl.when(known_status).then(pl.col("status")).otherwise(pl.lit("UNKNOWN")).alias("status")
        )
        state.add_rule("S-02", unknown_status)

        frame = self._dedupe(state, frame, ["entity_id"])
        frame = frame.with_columns(pl.col("created_at").dt.date().alias("created_date"))
        state.records_output = frame.height
        state.records_accepted = frame.height
        state.records_modified += unknown_count + unknown_status
        return frame

    def _canonicalise_payload(self, frame: pl.DataFrame, state: SourceState) -> tuple[pl.DataFrame, int]:
        if frame.height == 0:
            return frame, 0
        values: list[str] = []
        invalid = 0
        for value in frame["payload"].to_list():
            text = "" if value is None else str(value).strip()
            try:
                json.loads(text)
                values.append(text)
            except (json.JSONDecodeError, TypeError):
                values.append("{}")
                invalid += 1
        return frame.with_columns(pl.Series("payload", values, dtype=pl.String)), invalid

    def _clean_hb(self, raw: pl.DataFrame, state: SourceState, run_id: str, batch_id: str, bdate: date) -> tuple[pl.DataFrame, int]:
        required = [
            "HBT", "DMDCode", "BNFItemCode", "BNFItemDescription", "PrescribedType",
            "NumberOfPaidItems", "PaidQuantity", "GrossIngredientCost", "PaidDateMonth",
        ]
        self._require_columns(raw, required, "hb_pitc")
        state.records_read = raw.height
        state.parent_run_id = self._parent_for(raw)
        if raw.height == 0:
            state.records_output = 0
            return raw.with_columns(pl.lit(None, dtype=pl.Date).alias("paid_month")), 0

        frame = standardization.standardise_hb_columns(raw, raw.schema)

        frame = self._reject(
            state, frame,
            pl.col("PrescribedType").is_null() | (pl.col("PrescribedType") == "") | pl.col("PaidDateMonth").is_null(),
            "C-02", "Missing prescribed type or paid month", run_id, batch_id, bdate, state.parent_run_id,
        )
        frame = self._reject(
            state, frame,
            pl.col("NumberOfPaidItems").is_null() | pl.col("PaidQuantity").is_null() | pl.col("GrossIngredientCost").is_null(),
            "S-05", "Missing or non-numeric prescribing measure", run_id, batch_id, bdate, state.parent_run_id,
        )
        negative = (
            (pl.col("NumberOfPaidItems") < 0)
            | (pl.col("PaidQuantity") < 0)
            | (pl.col("GrossIngredientCost") < 0)
        )
        frame = self._reject(
            state, frame, negative,
            "C-07", "Negative paid items, quantity, or ingredient cost", run_id, batch_id, bdate, state.parent_run_id,
        )

        invalid_board = ~pl.col("HBT").fill_null("").str.contains(HBT_PATTERN.pattern)
        board_changes = frame.filter(invalid_board).height
        frame = frame.with_columns(
            pl.when(invalid_board).then(pl.lit(UNKNOWN_CODE)).otherwise(pl.col("HBT").fill_null(UNKNOWN_CODE)).alias("HBT")
        )
        state.add_rule("C-09", board_changes)

        # Unknown prescribed types are retained as UNKNOWN rather than dropped.
        unknown_type = frame.filter(~pl.col("PrescribedType").is_in(list(KNOWN_PRESCRIBED_TYPES))).height
        frame = frame.with_columns(
            pl.when(pl.col("PrescribedType").is_in(list(KNOWN_PRESCRIBED_TYPES)))
            .then(pl.col("PrescribedType"))
            .otherwise(pl.lit("UNKNOWN"))
            .alias("PrescribedType")
        )
        state.add_rule("C-06", unknown_type)

        frame = self._dedupe(
            state, frame, ["HBT", "DMDCode", "BNFItemCode", "PrescribedType", "PaidDateMonth"]
        )
        frame = frame.with_columns(
            pl.col("PaidDateMonth").dt.date().alias("paid_month"),
            pl.col("DMDCode").fill_null(NULL_SENTINEL_INT).alias("_dmd_key"),
            pl.col("BNFItemCode").fill_null(NULL_SENTINEL_STR).alias("_bnf_key"),
            (
                (pl.col("PaidQuantity") == 0) & (pl.col("NumberOfPaidItems") > 0)
            ).alias("_zero_quantity"),
        )
        zero_qty = frame.filter(pl.col("_zero_quantity")).height
        state.records_output = frame.height
        state.records_accepted = frame.height
        state.records_modified += board_changes + unknown_type
        return frame, zero_qty

    def _clean_device(self, raw: pl.DataFrame, state: SourceState, run_id: str, batch_id: str, bdate: date) -> pl.DataFrame:
        self._require_columns(raw, ["product_code", "device_name", "device_class", "last_updated"], "device_classification")
        state.records_read = raw.height
        state.parent_run_id = self._parent_for(raw)
        if raw.height == 0:
            state.records_output = 0
            return raw.with_columns(pl.lit(None, dtype=pl.Date).alias("last_updated_date"))

        frame = standardization.standardise_device_codes(raw)
        frame = self._reject(
            state, frame, pl.col("product_code").is_null() | (pl.col("product_code") == ""),
            "C-02", "Null or blank product_code", run_id, batch_id, bdate, state.parent_run_id,
        )
        frame = self._reject(
            state, frame, pl.col("last_updated").is_null(),
            "S-01", "Missing last_updated value", run_id, batch_id, bdate, state.parent_run_id,
        )
        frame = self._dedupe(state, frame, ["product_code"])
        frame = standardization.standardise_device_attributes(frame)
        state.records_output = frame.height
        state.records_accepted = frame.height
        return frame
