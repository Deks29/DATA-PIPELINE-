"""Configuration for the SightSync Stage 2 transformation engine.

Paths default to the same project layout used by Stage 1 so the transformation
engine can run locally and inside the Airflow Docker image. Everything is
overridable with ``SIGHTSYNC_TRANSFORM_*`` environment variables.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from extraction.config import PROJECT_ROOT, load_settings as load_extraction_settings


def _env_path(name: str) -> Path | None:
    raw = os.environ.get(name)
    return Path(raw).expanduser() if raw else None


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw else default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw else default


@dataclass
class TransformSettings:
    """Runtime settings resolved from the environment with local defaults."""

    base_dir: Path = PROJECT_ROOT
    staging_dir: Path = field(default=None)  # type: ignore[assignment]
    transformed_dir: Path = field(default=None)  # type: ignore[assignment]
    quarantine_dir: Path = field(default=None)  # type: ignore[assignment]
    logs_dir: Path = field(default=None)  # type: ignore[assignment]
    metadata_db: Path = field(default=None)  # type: ignore[assignment]

    pipeline_name: str = "sightsync_nightly_transform"
    transformation_version: str = "2.0.0"
    lookback_days: int = 3
    max_retries: int = 3

    # Section 10 - acceptance thresholds fixed in advance.
    rejected_ratio_threshold: float = 0.01
    max_rejected_ratio_threshold: float = 0.05

    # Offline/demo fallback: if the requested batch window has no partitions,
    # read the latest available partition instead of failing with no data.
    allow_latest_fallback: bool = True

    def __post_init__(self) -> None:
        extraction = load_extraction_settings()
        self.base_dir = Path(self.base_dir)
        self.staging_dir = self.staging_dir or extraction.staging_dir
        self.transformed_dir = self.transformed_dir or (self.base_dir / "data" / "transformed")
        self.quarantine_dir = self.quarantine_dir or (self.base_dir / "data" / "quarantine")
        self.logs_dir = self.logs_dir or (self.base_dir / "data" / "logs" / "transformation")
        self.metadata_db = self.metadata_db or (
            self.base_dir / "data" / "metadata" / "sightsync_transform_audit.db"
        )

    def transformed_dir_for(self, table: str) -> Path:
        return Path(self.transformed_dir) / table

    def quarantine_dir_for(self, table: str) -> Path:
        return Path(self.quarantine_dir) / table


def load_transform_settings(**overrides) -> TransformSettings:
    extraction = load_extraction_settings()
    enabled = os.environ.get("SIGHTSYNC_TRANSFORM_ALLOW_LATEST_FALLBACK", "true").lower()
    settings = TransformSettings(
        base_dir=extraction.base_dir,
        staging_dir=_env_path("SIGHTSYNC_TRANSFORM_STAGING_DIR") or extraction.staging_dir,
        transformed_dir=_env_path("SIGHTSYNC_TRANSFORM_OUTPUT_DIR"),
        quarantine_dir=_env_path("SIGHTSYNC_TRANSFORM_QUARANTINE_DIR"),
        logs_dir=_env_path("SIGHTSYNC_TRANSFORM_LOGS_DIR"),
        metadata_db=_env_path("SIGHTSYNC_TRANSFORM_METADATA_DB"),
        pipeline_name=os.environ.get("SIGHTSYNC_TRANSFORM_PIPELINE_NAME", "sightsync_nightly_transform"),
        transformation_version=os.environ.get("SIGHTSYNC_TRANSFORM_VERSION", "2.0.0"),
        lookback_days=_env_int("SIGHTSYNC_TRANSFORM_LOOKBACK_DAYS", extraction.lookback_days),
        max_retries=_env_int("SIGHTSYNC_TRANSFORM_MAX_RETRIES", extraction.max_retries),
        rejected_ratio_threshold=_env_float(
            "SIGHTSYNC_TRANSFORM_REJECTED_RATIO_THRESHOLD", extraction.rejected_ratio_threshold
        ),
        max_rejected_ratio_threshold=_env_float(
            "SIGHTSYNC_TRANSFORM_MAX_REJECTED_RATIO_THRESHOLD", 0.05
        ),
        allow_latest_fallback=enabled not in ("0", "false", "no"),
    )
    for key, value in overrides.items():
        setattr(settings, key, value)
    return settings
