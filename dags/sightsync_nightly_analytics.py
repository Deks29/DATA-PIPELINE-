"""Airflow DAG: SightSync Nightly Analytics - Stage 1 (Data Extraction).

Schedule: daily at 02:00 UTC / 10:00 Asia/Manila.
Retries:  up to 3 times before triggering the engineer alert (Section 7).

The DAG is a thin orchestrator: all extraction logic lives in the standalone
``extraction`` package so it can be unit tested and run without Airflow.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException

from extraction.config import load_settings
from extraction.metadata import MetadataStore
from extraction.pipeline import (
    APPROVED,
    APPROVED_WITH_WARNING,
    MANUAL_REVIEW,
    REJECTED,
    ExtractionPipeline,
    to_json,
)

log = logging.getLogger(__name__)

SETTINGS = load_settings()

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": SETTINGS.max_retries,
    "retry_delay": timedelta(minutes=10),
}


with DAG(
    dag_id="sightsync_nightly_analytics",
    description="SightSync Stage 1 - Polars data extraction to Parquet staging",
    default_args=default_args,
    schedule="0 2 * * *",  # 02:00 UTC == 10:00 Asia/Manila
    start_date=datetime(2026, 10, 1),
    catchup=True,
    max_active_runs=1,
    tags=["sightsync", "stage-1", "extraction", "polars"],
) as dag:

    @task
    def extract(**context) -> list[dict]:
        """Extract raw_logs and raw_entities for the DAG's logical business date."""
        business_date = context["ds"]  # Airflow logical date, i.e. the business date
        pipeline = ExtractionPipeline(settings=SETTINGS)
        results = pipeline.run(business_date)
        log.info("Extraction summary:\n%s", to_json(results))
        return [
            {
                "table_name": r.table_name,
                "decision": r.decision,
                "rows_read": r.rows_read,
                "rows_extracted": r.rows_extracted,
                "rows_rejected": r.rows_rejected,
                "validation_results": r.check_map,
            }
            for r in results
        ]

    @task
    def evaluate(summaries: list[dict]) -> None:
        """Apply Section 7 handover decision rules and gate the pipeline."""
        rejected = [s for s in summaries if s["decision"] == REJECTED]
        manual = [s for s in summaries if s["decision"] == MANUAL_REVIEW]

        if rejected:
            raise AirflowFailException(
                "REJECTED - halting pipeline. "
                + "; ".join(f"{s['table_name']}: {s['validation_results']}" for s in rejected)
            )
        if manual:
            raise AirflowFailException(
                "MANUAL REVIEW required - rows_rejected > 1% of rows_read. "
                + "; ".join(s["table_name"] for s in manual)
                + " (Data Lead approval required before PySpark)."
            )
        for s in summaries:
            if s["decision"] == APPROVED_WITH_WARNING:
                log.warning(
                    "APPROVED WITH WARNING for %s: %s",
                    s["table_name"],
                    json.dumps(s["validation_results"]),
                )
        log.info("All tables APPROVED - handing Parquet staging files to PySpark.")

    @task
    def purge_retention() -> dict:
        """Section 5 - enforce operational log / audit retention policy."""
        store = MetadataStore(SETTINGS.metadata_db, SETTINGS.logs_dir)
        removed = store.purge_expired(
            SETTINGS.operational_log_retention_days, SETTINGS.audit_retention_months
        )
        log.info("Retention purge: %s", removed)
        return removed

    summaries = extract()
    evaluate(summaries)
    purge_retention()
