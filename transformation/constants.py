"""Shared constants, output contracts and key helpers for Stage 2."""

from __future__ import annotations

import hashlib
import re
from datetime import date, datetime, timezone
from typing import Any

import polars as pl

APPROVED = "APPROVED"
APPROVED_WITH_WARNING = "APPROVED WITH WARNING"
MANUAL_REVIEW = "MANUAL REVIEW"
REJECTED = "REJECTED"

PASS = "PASS"
FAIL = "FAIL"
WARNING = "WARNING"
SKIP = "SKIP"
CRITICAL = "Critical"
NON_CRITICAL = "Non-critical"

UNKNOWN_USER = "UNKNOWN_USER"
UNKNOWN_CODE = "UNKNOWN"
AGGREGATE_CODE = "AGGREGATE"
NULL_SENTINEL_INT = -1
NULL_SENTINEL_STR = "__NULL__"

KNOWN_PRESCRIBED_TYPES = {
    "AMP": (1, "Actual Medicinal Product"),
    "VMP": (2, "Virtual Medicinal Product"),
    "AMPP": (3, "Actual Medicinal Product Pack"),
    "VMPP": (4, "Virtual Medicinal Product Pack"),
    "UNKNOWN": (0, "Unknown prescribed type"),
}

HBT_PATTERN = re.compile(r"^S(?:B)?\d{4,8}$")
DATE_PARTITION = re.compile(r"business_date=(\d{4}-\d{2}-\d{2})")

OUTPUT_COLUMNS: dict[str, list[str]] = {
    "dim_date": [
        "date_key", "calendar_date", "year", "quarter", "month", "day",
        "week_of_year", "is_weekend",
    ],
    "dim_user": [
        "user_key", "user_id", "first_seen_at", "last_seen_at",
        "user_is_unknown", "updated_at",
    ],
    "dim_health_board": [
        "health_board_key", "health_board_code", "health_board_name", "is_unknown",
    ],
    "dim_drug": [
        "drug_key", "dmd_code", "bnf_item_code", "bnf_item_description",
        "is_aggregate", "updated_at",
    ],
    "dim_prescribed_type": [
        "prescribed_type_key", "prescribed_type_code",
        "prescribed_type_description",
    ],
    "dim_device_classification": [
        "device_key", "product_code", "device_name", "device_class",
        "regulation_number", "medical_specialty_description", "review_panel",
        "implant_flag", "life_sustain_support_flag", "gmp_exempt_flag",
        "third_party_flag", "submission_type_id", "definition", "last_updated",
        "updated_at",
    ],
    "fact_prescribing_monthly": [
        "prescribing_key", "health_board_key", "drug_key",
        "prescribed_type_key", "period_key", "paid_month", "paid_items",
        "paid_quantity", "gross_ingredient_cost", "avg_cost_per_item",
        "avg_cost_per_quantity", "zero_quantity_flag", "transformed_run_id",
        "updated_at",
    ],
    "fact_event_daily": [
        "event_fact_key", "event_date_key", "user_key", "event_type",
        "event_count", "transformed_run_id",
    ],
    "fact_daily_summary": [
        "summary_key", "event_date_key", "total_events", "unique_users",
        "signup_count", "purchase_count", "view_count", "click_count",
        "logout_count", "entities_created", "entities_active",
        "entities_completed", "entities_cancelled", "transformed_run_id",
    ],
}

PRIMARY_KEYS = {
    "dim_date": ["date_key"],
    "dim_user": ["user_key"],
    "dim_health_board": ["health_board_key"],
    "dim_drug": ["drug_key"],
    "dim_prescribed_type": ["prescribed_type_key"],
    "dim_device_classification": ["device_key"],
    "fact_prescribing_monthly": ["prescribing_key"],
    "fact_event_daily": ["event_fact_key"],
    "fact_daily_summary": ["summary_key"],
}

REQUIRED_COLUMNS = {
    "dim_date": ["date_key", "calendar_date", "year", "month", "day"],
    "dim_user": ["user_key", "user_id", "first_seen_at", "last_seen_at"],
    "dim_health_board": ["health_board_key", "health_board_code"],
    "dim_drug": ["drug_key", "is_aggregate", "updated_at"],
    "dim_prescribed_type": ["prescribed_type_key", "prescribed_type_code"],
    "dim_device_classification": ["device_key", "product_code", "last_updated"],
    "fact_prescribing_monthly": [
        "prescribing_key", "health_board_key", "drug_key",
        "prescribed_type_key", "period_key", "paid_month", "paid_items",
        "paid_quantity", "gross_ingredient_cost",
    ],
    "fact_event_daily": ["event_fact_key", "event_date_key", "user_key", "event_type", "event_count"],
    "fact_daily_summary": ["summary_key", "event_date_key", "total_events", "unique_users"],
}

EMPTY_SCHEMAS: dict[str, dict[str, pl.DataType]] = {
    "dim_device_classification": {
        "device_key": pl.Int64(),
        "product_code": pl.String(),
        "device_name": pl.String(),
        "device_class": pl.String(),
        "regulation_number": pl.String(),
        "medical_specialty_description": pl.String(),
        "review_panel": pl.String(),
        "implant_flag": pl.Boolean(),
        "life_sustain_support_flag": pl.Boolean(),
        "gmp_exempt_flag": pl.Boolean(),
        "third_party_flag": pl.Boolean(),
        "submission_type_id": pl.String(),
        "definition": pl.String(),
        "last_updated": pl.Date(),
        "updated_at": pl.Datetime("us", "UTC"),
    },
    "fact_prescribing_monthly": {
        "prescribing_key": pl.Int64(),
        "health_board_key": pl.Int64(),
        "drug_key": pl.Int64(),
        "prescribed_type_key": pl.Int64(),
        "period_key": pl.Int64(),
        "paid_month": pl.Date(),
        "paid_items": pl.Int64(),
        "paid_quantity": pl.Float64(),
        "gross_ingredient_cost": pl.Float64(),
        "avg_cost_per_item": pl.Float64(),
        "avg_cost_per_quantity": pl.Float64(),
        "zero_quantity_flag": pl.Boolean(),
        "transformed_run_id": pl.String(),
        "updated_at": pl.Datetime("us", "UTC"),
    },
    "fact_event_daily": {
        "event_fact_key": pl.Int64(),
        "event_date_key": pl.Int64(),
        "user_key": pl.Int64(),
        "event_type": pl.String(),
        "event_count": pl.Int64(),
        "transformed_run_id": pl.String(),
    },
    "fact_daily_summary": {
        "summary_key": pl.Int64(),
        "event_date_key": pl.Int64(),
        "total_events": pl.Int64(),
        "unique_users": pl.Int64(),
        "signup_count": pl.Int64(),
        "purchase_count": pl.Int64(),
        "view_count": pl.Int64(),
        "click_count": pl.Int64(),
        "logout_count": pl.Int64(),
        "entities_created": pl.Int64(),
        "entities_active": pl.Int64(),
        "entities_completed": pl.Int64(),
        "entities_cancelled": pl.Int64(),
        "transformed_run_id": pl.String(),
    },
}


def empty_frame(table_key: str) -> pl.DataFrame:
    return pl.DataFrame(schema=EMPTY_SCHEMAS[table_key])


def stable_key(namespace: str, *values: Any) -> int:
    joined = "|".join([namespace, *("" if v is None else str(v) for v in values)])
    digest = hashlib.blake2b(joined.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") & 0x7FFFFFFFFFFFFFFF


def date_key(value: date) -> int:
    return value.year * 10000 + value.month * 100 + value.day


def date_from_key(value: int) -> date:
    text = str(value)
    return date(int(text[:4]), int(text[4:6]), int(text[6:8]))


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
