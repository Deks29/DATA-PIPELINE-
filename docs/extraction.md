# Data Extraction Documentation

*BAT-405 Analytics Application Capstone — Laboratory Activity 3*

*Authors:* John Patrick Russel D. Lalo, Dexter T. Manalo, Hazel Ann D. Manimtim


## Objective

Extract one business day of records from SightSync operational sources (raw CSV/JSON log files and transactional records), verify that the extraction is complete and reliable, and package it for the Transformation stage (Polars analytics engine).

The pipeline is a standalone Python application built using the Polars engine and orchestrated via Apache Airflow (in Docker). Stage 1 does not alter source files or databases in any way.

This stage does not aggregate or calculate business summaries. Daily totals, unique users, and category metrics are computed downstream during Transformation using the Polars analytics engine.

## Scope Decisions & Comparison with Lab 2 Specification

To keep ETL efficient and avoid unnecessary memory overhead, two core architectural decisions were aligned with Lab 2 and implemented for Lab 3:

1. *Core Four Datasets Scope:* Lab 2 specified raw event logs and transactional entities. Tables or log streams without a direct Downstream Consumer or Target Data Mart (analytics.fct_daily_summary) were excluded from Stage 1 extraction scope.
2. *Local Polars Parquet Staging:* Raw CSV/JSON logs are converted immediately into standardized, compressed Parquet staging files using Polars (Rust engine) to ensure fast I/O speed and minimal RAM utilization before handing data off to Apache Spark.

## 1. Data Source and Extraction Specification

### 1.1 Source System

- *Name:* SightSync Operational & Event Logging System
- *Purpose:* Records user interaction logs, transactional records, and system metrics.
- *Role in Pipeline:* System of record. Serves as the single source for the analytical batch pipeline.
- *Access Behavior:* Read-only extraction. No raw logs or operational databases are modified.

### 1.2 Source Format & Connection

- *File Formats:* Raw CSV and JSON event log drops.
- *Ingestion Engine:* Polars (High-performance Rust-based data manipulation library).
- *Output Staging Format:* Compressed Parquet Staging Files.
- *Connection / Storage Access:* Local File System / Object Storage (Read-Only).

### 1.3 Extraction Method

- *Type:* Incremental by business date (Batch-based).
- *Tool:* Python 3.12 script using Polars, scheduled and monitored by Apache Airflow (in Docker).
- *Schedule:* Daily at 02:00 UTC / 10:00 Asia/Manila.
- *Extraction Window:* Driven by business_date calendar window (00:00:00 UTC to 23:59:59 UTC). A 3-day trailing look-back window is applied to capture late-arriving logs.

## 2. Source Tables and Column Specification

### 2.1 staging.raw_logs (Polars Ingested)

Contains raw user event logs. *Primary Key:* log_id (String). Primary source for event counts and unique users in analytics.fct_daily_summary.

| Column | Source Type | Target Type | Purpose |
| --- | --- | --- | --- |
| log_id | String | String | Primary Key. Unique identifier for the log record. |
| event_type | String | String | Event classification (e.g., click, view). |
| user_id | String | String | Unique user identifier. |
| timestamp | Timestamp | Timestamp | Event timestamp (Drives extraction window). |
| payload | String | String | Additional JSON event metadata. |

### 2.2 staging.raw_entities (Polars Ingested)

Entity and transactional metadata. *Primary Key:* entity_id (String). Source for monetary total value calculations.

| Column | Source Type | Target Type | Purpose |
| --- | --- | --- | --- |
| entity_id | String | String | Primary Key. Identifies entity record. |
| user_id | String | String | Foreign Link to user table. |
| status | String | String | Latest status of the entity (e.g., active, completed). |
| created_at | Timestamp | Timestamp | Record creation timestamp. |

### 2.3 staging.hb_pitc (Real Data Source)

NHS Health Board prescribing data supplied as `hb_pitc<YYYY>_<MM>.csv` (for example `hb_pitc2026_07.csv`). The grain is health board x dispensed item x prescribed type x paid month. *Composite Primary Key:* HBT + DMDCode + BNFItemCode + PrescribedType + PaidDateMonth. BNF/DMD codes are null for aggregate rows, so null key values are permitted for this contract.

| Column | Source Type | Target Type | Purpose |
| --- | --- | --- | --- |
| HBT | String | String | Health board code. |
| DMDCode | Integer | Integer | Dictionary of Medicines and Devices code (null for aggregates). |
| BNFItemCode | String | String | BNF item code (null for aggregates). |
| BNFItemDescription | String | String | BNF item description. |
| PrescribedType | String | String | AMP, VMP, AMPP or VMPP. |
| NumberOfPaidItems | Integer | Integer | Paid item count. |
| PaidQuantity | Float | Float | Paid quantity. |
| GrossIngredientCost | Float | Float | Gross ingredient cost. |
| PaidDateMonth | Integer (YYYYMM) | Timestamp (UTC, month start) | Period used for the monthly extraction window and business_date partition. |

The extraction window for this table is the full calendar month of `business_date`; the 3-day trailing look-back applies only to the daily log tables.

### 2.4 staging.device_classification (openFDA REST API)

Device classification snapshot pulled from the openFDA REST API configured by the `API` entry in `.env` (default `https://api.fda.gov/device/classification.json`, search `product_code:NOB`, page size 500). The client follows openFDA pagination, then writes the normalised records to `data/raw/device_classification/device_classification.json` before the standard V1–V7 checks run. The record timestamp is the API's `meta.last_updated` date, and the extraction window is the full calendar month of `business_date`. *Primary Key:* product_code.

| Column | Source Type | Target Type | Purpose |
| --- | --- | --- | --- |
| product_code | String | String | Primary Key. FDA product code. |
| device_name | String | String | Device classification name. |
| device_class | String | String | FDA device class (1, 2, 3, etc.). |
| regulation_number | String | String | Applicable regulation number. |
| medical_specialty_description | String | String | Medical specialty description. |
| review_panel | String | String | Review panel code. |
| implant_flag | String | String | Implant flag (Y/N). |
| life_sustain_support_flag | String | String | Life-sustaining/support flag (Y/N). |
| gmp_exempt_flag | String | String | GMP exemption flag (Y/N). |
| third_party_flag | String | String | Third-party review flag (Y/N). |
| submission_type_id | String | String | Submission type identifier. |
| definition | String | String | Device definition text. |
| last_updated | String (YYYY-MM-DD) | Timestamp (UTC, midnight) | API dataset update date; drives the monthly window. |

## 3. Extraction Validation and Data Quality Checks

| # | Check Name | Target | Criteria | Severity |
| --- | --- | --- | --- | --- |
| V1 | Source File Access | Raw CSV/JSON files | Source files exist and are readable in storage path. | Critical |
| V2 | Schema Drift Validation | Source Files | All required columns exist with matching data types during Polars parse. | Critical |
| V3 | Window Resolution | Business Date | business_date is valid and no prior successful run exists in Airflow audit log. | Critical |
| V4 | Key Integrity | Primary Keys | Zero Null values in Primary Keys for raw_logs and raw_entities. | Critical |
| V5 | Primary Key Uniqueness | Primary Keys | count(distinct log_id) == count(log_id). | Critical |
| V6 | Extraction Parquet Parity | Read vs Written Rows | rows_read == rows_extracted + rows_rejected. | Critical |
| V7 | Non-Empty File Check | raw_logs | At least 1 log entry present in current extraction window. | Warning |

## 4. Extraction Metadata and Log Specification

Each run of the Polars extraction script under Airflow logs an audit entry to the Metadata Log Table and writes a local JSON log file:

| Field Name | Purpose | Example |
| --- | --- | --- |
| run_id | Unique Airflow DAG execution UUID. | a8f3b1e2-9c10-4e88-b223-112233445566 |
| run_key | Idempotency key for safe execution retries. | sightsync_nightly_analytics:2026-10-06 |
| pipeline_name | Name of the pipeline. | sightsync_nightly_analytics |
| business_date | Date of data processed. | 2026-10-06 |
| table_name | Staging table/file extracted. | staging.raw_logs |
| extraction_started_at | UTC timestamp when Polars parse started. | 2026-10-07T02:00:01Z |
| extraction_ended_at | UTC timestamp when Parquet write completed. | 2026-10-07T02:00:15Z |
| rows_read | Total rows read from raw CSV/JSON files. | 1,250,000 |
| rows_extracted | Total rows successfully exported to Parquet. | 1,250,000 |
| rows_rejected | Rows dropped due to schema/null key errors. | 0 |
| validation_results | JSON status mapping of V1–V7 checks. | {"V1":"PASS", "V2":"PASS", ..., "V7":"PASS"} |

## 5. Log Retention and Access Policy

- *Operational Log Retention:* Retained for 90 days in the Airflow database and local execution logs.
- *Audit Record Retention:* Retained for 24 months (2 years) for historical audit verification.
- *Storage Location:* Compressed local storage directory and dedicated Airflow metadata database.
- *Access Permissions:* Write: Airflow Execution Agent / Service Account. Read: Data Engineering Team and System Administrators.

## 6. Extraction Data Contract & Target Schema Mappings

### 6.1 Control Columns

Every Parquet Staging file generated by Polars includes three standard Control Columns:

- **run_id** (String): Links the row directly to the Airflow DAG run.
- **business_date** (Date): The calendar date of the analytics window.
- **extracted_at** (Timestamp): UTC timestamp recording when the Parquet file was generated.

### 6.2 Naming & Type Mapping

- log_id → log_id (String | Pass-through)
- event_type → event_type (String | Pass-through)
- user_id → user_id (String | Pass-through)
- timestamp → timestamp (Timestamp | Standardized UTC)
- payload → payload (String | Pass-through)

## 7. Handover Acceptance and Decision Rules

| Overall Condition | Decision | Pipeline Action |
| --- | --- | --- |
| All Critical Checks (V1–V6) PASS; no warnings | APPROVED | Automatically pass Parquet staging files to Stage 2 (Polars transformation). |
| All Critical Checks PASS; V7 Warning present | APPROVED WITH WARNING | Automatically pass to Stage 2; dispatch alert notification to Airflow UI. |
| Any Critical Check (V1–V6) FAILS | REJECTED | Halt pipeline execution. Airflow retries up to 3 times before triggering engineer alert. |
| rows_rejected > 1% of rows_read | MANUAL REVIEW | Pause DAG run. Requires Data Lead approval before proceeding to Stage 2. |

## 8. Common Extraction Problems and Mitigation Strategies

*P1: High RAM Usage on Large Log Files* — Loading large unstructured log files directly into memory causing OOM crashes.
Mitigation: Use Polars LazyFrames (pl.scan_csv(), pl.scan_ndjson()) for streaming batch ingestion without reading entire files into RAM.

*P2: Schema Drift or Missing Required Columns* — Upstream log format modifications causing transformation errors downstream.
Mitigation: Check V2 (Schema Drift Validation) halts Airflow DAG execution immediately if schema types do not match the Lab 2 contract.

*P3: Duplicate Row Processing During Re-runs* — Manual re-triggers of Airflow DAGs for historical dates causing duplicate rows.
Mitigation: Partition Parquet directories by date (/staging/raw_logs/business_date=YYYY-MM-DD/). Re-runs perform an atomic folder overwrite, ensuring full pipeline idempotency.
