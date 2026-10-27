"""SightSync Batch Pipeline - Stage 1 (Data Extraction).

A standalone Python application that extracts one business day of records from
SightSync operational sources using the Polars engine, validates the extraction
(V1-V7), and packages it as compressed Parquet staging files for the
Transformation stage (PySpark & dbt Core).

This stage is read-only: it never alters source files or databases.
"""

from extraction.config import Settings, TableSpec, TABLES, load_settings

__all__ = ["Settings", "TableSpec", "TABLES", "load_settings"]
