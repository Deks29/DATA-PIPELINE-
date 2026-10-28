"""SightSync Stage 2 - Data Transformation.

Reads validated Parquet staging output from Stage 1, applies cleaning,
standardization and business rules, validates the result, and writes the
``analytics`` dimensional model as Parquet for the Load stage.
"""

from transformation.config import TransformSettings, load_transform_settings
from transformation.pipeline import (
    APPROVED,
    APPROVED_WITH_WARNING,
    MANUAL_REVIEW,
    REJECTED,
    TransformationPipeline,
    TransformationResult,
    to_json,
)

__all__ = [
    "APPROVED",
    "APPROVED_WITH_WARNING",
    "MANUAL_REVIEW",
    "REJECTED",
    "TransformSettings",
    "TransformationPipeline",
    "TransformationResult",
    "load_transform_settings",
    "to_json",
]
