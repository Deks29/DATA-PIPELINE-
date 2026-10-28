"""Configuration, source table contracts and settings for the extraction stage.

Values mirror the Lab 3 Data Extraction Documentation (SightSync). Everything is
overridable through ``SIGHTSYNC_*`` environment variables so the same engine runs
locally and inside the Airflow Docker container.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Load local .env (real environment variables take precedence over the file).
load_dotenv(PROJECT_ROOT / ".env", override=False)

ColumnKind = Literal["string", "timestamp", "integer", "float"]
TimestampFormat = Literal["iso", "yyyymm", "date"]
WindowMode = Literal["trailing", "month"]
SourceKind = Literal["file", "api"]


@dataclass(frozen=True)
class TableSpec:
    """Contract for a single staging table (source table + column specification)."""

    name: str
    file_glob: str
    primary_keys: tuple[str, ...]
    timestamp_column: str
    required_columns: dict[str, ColumnKind]
    partition_column: str = "business_date"
    timestamp_format: TimestampFormat = "iso"
    window_mode: WindowMode = "trailing"
    allow_null_keys: bool = False
    source: SourceKind = "file"
    api_search: str | None = None

    @property
    def primary_key(self) -> str:
        return self.primary_keys[0]

    @property
    def string_columns(self) -> list[str]:
        return [c for c, k in self.required_columns.items() if k == "string"]

    @property
    def timestamp_columns(self) -> list[str]:
        return [c for c, k in self.required_columns.items() if k == "timestamp"]


# Section 2 - Source Tables and Column Specification

RAW_LOGS = TableSpec(
    name="staging.raw_logs",
    file_glob="raw_logs_*.{ext}",
    primary_keys=("log_id",),
    timestamp_column="timestamp",
    required_columns={
        "log_id": "string",
        "event_type": "string",
        "user_id": "string",
        "timestamp": "timestamp",
        "payload": "string",
    },
)

RAW_ENTITIES = TableSpec(
    name="staging.raw_entities",
    file_glob="raw_entities_*.{ext}",
    primary_keys=("entity_id",),
    timestamp_column="created_at",
    required_columns={
        "entity_id": "string",
        "user_id": "string",
        "status": "string",
        "created_at": "timestamp",
    },
)

# NHS Health Board prescribing data (HB PITC). The grain is health board x
# dispensed item x prescribed type x paid month; BNF/DMD codes are null for
# aggregate rows, so null key values are allowed by this contract.
HB_PITC = TableSpec(
    name="staging.hb_pitc",
    file_glob="hb_pitc*.{ext}",
    primary_keys=("HBT", "DMDCode", "BNFItemCode", "PrescribedType", "PaidDateMonth"),
    timestamp_column="PaidDateMonth",
    timestamp_format="yyyymm",
    window_mode="month",
    allow_null_keys=True,
    required_columns={
        "HBT": "string",
        "DMDCode": "integer",
        "BNFItemCode": "string",
        "BNFItemDescription": "string",
        "PrescribedType": "string",
        "NumberOfPaidItems": "integer",
        "PaidQuantity": "float",
        "GrossIngredientCost": "float",
        "PaidDateMonth": "timestamp",
    },
)

TABLES: dict[str, TableSpec] = {
    "raw_logs": RAW_LOGS,
    "raw_entities": RAW_ENTITIES,
    "hb_pitc": HB_PITC,
}

# openFDA device classification snapshot pulled from the REST API configured by
# the ``API`` environment variable (see .env). The API has no event timestamp,
# so the record carries the API's ``meta.last_updated`` date, and a monthly
# window keeps the snapshot idempotent per business month.
DEVICE_CLASSIFICATION = TableSpec(
    name="staging.device_classification",
    file_glob="device_classification.{ext}",
    primary_keys=("product_code",),
    timestamp_column="last_updated",
    timestamp_format="date",
    window_mode="month",
    source="api",
    api_search="product_code:NOB",
    required_columns={
        "product_code": "string",
        "device_name": "string",
        "device_class": "string",
        "regulation_number": "string",
        "medical_specialty_description": "string",
        "review_panel": "string",
        "implant_flag": "string",
        "life_sustain_support_flag": "string",
        "gmp_exempt_flag": "string",
        "third_party_flag": "string",
        "submission_type_id": "string",
        "definition": "string",
        "last_updated": "timestamp",
    },
)

TABLES["device_classification"] = DEVICE_CLASSIFICATION

# Section 1.2 - Source Format & Connection
SUPPORTED_EXTENSIONS = ("csv", "json")


def _env_path(name: str, default: Path) -> Path:
    raw = os.environ.get(name)
    return Path(raw).expanduser() if raw else default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw else default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw else default


@dataclass
class Settings:
    """Runtime settings resolved from environment with sensible local defaults."""

    base_dir: Path = PROJECT_ROOT
    raw_dir: Path = field(default=None)  # type: ignore[assignment]
    staging_dir: Path = field(default=None)  # type: ignore[assignment]
    logs_dir: Path = field(default=None)  # type: ignore[assignment]
    metadata_db: Path = field(default=None)  # type: ignore[assignment]

    pipeline_name: str = "sightsync_nightly_analytics"
    # Section 1.3 - Extraction Window: 3-day trailing look-back for late logs.
    lookback_days: int = 3
    schedule_hour_utc: int = 2
    timezone: str = "UTC"

    # Section 5 - Log Retention and Access Policy
    operational_log_retention_days: int = 90
    audit_retention_months: int = 24

    # Section 7 - Handover Acceptance and Decision Rules
    rejected_ratio_threshold: float = 0.01
    max_retries: int = 3

    # openFDA REST API source (device classification snapshot).
    api_url: str = ""
    api_search: str = "product_code:NOB"
    api_limit: int = 500
    api_key: str = ""

    def __post_init__(self) -> None:
        self.raw_dir = self.raw_dir or (self.base_dir / "data" / "raw")
        self.staging_dir = self.staging_dir or (self.base_dir / "data" / "staging")
        self.logs_dir = self.logs_dir or (self.base_dir / "data" / "logs")
        self.metadata_db = self.metadata_db or (self.base_dir / "data" / "metadata" / "sightsync_audit.db")

    def raw_dir_for(self, table: str) -> Path:
        return Path(self.raw_dir) / table

    def staging_dir_for(self, table: str) -> Path:
        return Path(self.staging_dir) / table


def load_settings(**overrides) -> Settings:
    """Build a :class:`Settings` from environment variables, then apply overrides."""

    base_dir = _env_path("SIGHTSYNC_BASE_DIR", PROJECT_ROOT)
    settings = Settings(
        base_dir=base_dir,
        raw_dir=_env_path("SIGHTSYNC_RAW_DIR", base_dir / "data" / "raw")
        if os.environ.get("SIGHTSYNC_RAW_DIR")
        else None,
        staging_dir=_env_path("SIGHTSYNC_STAGING_DIR", base_dir / "data" / "staging")
        if os.environ.get("SIGHTSYNC_STAGING_DIR")
        else None,
        logs_dir=_env_path("SIGHTSYNC_LOGS_DIR", base_dir / "data" / "logs")
        if os.environ.get("SIGHTSYNC_LOGS_DIR")
        else None,
        metadata_db=_env_path("SIGHTSYNC_METADATA_DB", base_dir / "data" / "metadata" / "sightsync_audit.db")
        if os.environ.get("SIGHTSYNC_METADATA_DB")
        else None,
        pipeline_name=os.environ.get("SIGHTSYNC_PIPELINE_NAME", "sightsync_nightly_analytics"),
        lookback_days=_env_int("SIGHTSYNC_LOOKBACK_DAYS", 3),
        schedule_hour_utc=_env_int("SIGHTSYNC_SCHEDULE_HOUR_UTC", 2),
        timezone=os.environ.get("SIGHTSYNC_TIMEZONE", "UTC"),
        operational_log_retention_days=_env_int("SIGHTSYNC_LOG_RETENTION_DAYS", 90),
        audit_retention_months=_env_int("SIGHTSYNC_AUDIT_RETENTION_MONTHS", 24),
        rejected_ratio_threshold=_env_float("SIGHTSYNC_REJECTED_RATIO_THRESHOLD", 0.01),
        max_retries=_env_int("SIGHTSYNC_MAX_RETRIES", 3),
        api_url=os.environ.get("API", os.environ.get("SIGHTSYNC_API_URL", "")),
        api_search=os.environ.get("API_SEARCH", os.environ.get("SIGHTSYNC_API_SEARCH", "product_code:NOB")),
        api_limit=_env_int("API_LIMIT", _env_int("SIGHTSYNC_API_LIMIT", 500)),
        api_key=os.environ.get("API_KEY", os.environ.get("SIGHTSYNC_API_KEY", "")),
    )
    for key, value in overrides.items():
        setattr(settings, key, value)
    return settings
