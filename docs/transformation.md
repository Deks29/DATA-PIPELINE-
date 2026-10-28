# Stage 2: Data Transformation Plan

*BAT-405 Analytics Application Capstone — Laboratory Activity 4*

*Authors:* John Patrick Russel D. Lalo, Dexter T. Manalo, Hazel Ann D. Manimtim

## Objective

Transform the validated Stage 1 extraction handover into a cleaned, standardized,
business-ready dataset for the Load stage. The output must be documented,
traceable, reconciled against its input, and validated before handover.

This plan is limited to transformation. It does not re-extract data from source
systems and does not load data into the target warehouse. All transformation
outputs are written as partitioned Parquet files and described by the contracts
in Section 6 so the Load stage can consume them without re-deriving business
logic.

---

## 1. Transformation Input and Processing Specification

### 1.1 Input Source

Stage 2 receives only the Parquet staging output produced by the Stage 1
extraction engine. The source system is **not** read again.

| Input table | Staging location | Format | Natural key(s) |
| --- | --- | --- | --- |
| `staging.raw_logs` | `data/staging/raw_logs/business_date=YYYY-MM-DD/data.parquet` | Parquet (zstd) | `log_id` |
| `staging.raw_entities` | `data/staging/raw_entities/business_date=YYYY-MM-DD/data.parquet` | Parquet (zstd) | `entity_id` |
| `staging.hb_pitc` | `data/staging/hb_pitc/business_date=YYYY-MM-01/data.parquet` | Parquet (zstd) | `HBT`, `DMDCode`, `BNFItemCode`, `PrescribedType`, `PaidDateMonth` |
| `staging.device_classification` | `data/staging/device_classification/business_date=YYYY-MM-DD/data.parquet` | Parquet (zstd) | `product_code` |

Every staging row carries the extraction control columns `run_id`,
`business_date`, and `extracted_at`. Stage 2 links its run to the extraction run
through `parent_run_id` rather than copying extraction metadata.

The extraction audit is read from:

- Metadata: `data/metadata/sightsync_audit.db` (`extraction_runs` table)
- Operational logs: `data/logs/<business_date>/<run_id>_<table>.json`
- Handover decision: `APPROVED` or `APPROVED WITH WARNING`

Example handover: Stage 1 extracted `raw_logs` and `raw_entities` for
`business_date=2026-10-06` under extraction run `EXT_...`; Stage 2 reads that
partition, profiles it, applies rules `C-*`, `S-*`, and `B-*`, and writes the
transformed daily summary for the same business date.

### 1.2 Batch Processing Method

| Item | Specification |
| --- | --- |
| Transformation technology | Polars batch engine in the standalone `transformation` Python package, orchestrated by Airflow; rule and contract checks are implemented as automated unit tests (`tests/test_transformation.py`) |
| Scheduler | Daily at 02:00 UTC / 10:00 Asia-Manila, immediately after the Stage 1 extraction DAG completes successfully |
| Log / entity window | Read the current `business_date` partition **plus the 3-day trailing look-back** already present in staging; recompute every affected event/created date |
| HB PITC window | Read the current month partition and the previous month partition; recompute both monthly facts to absorb late corrections |
| Device classification window | Read the latest snapshot; apply SCD Type 1 when `last_updated` or a source attribute changes |
| Batch size | One extraction business date for logs/entities, one or two calendar months for PITC, one full API snapshot for device classification |
| Rerun strategy | Airflow retry (up to 3, 10-minute delay); rerun deletes and re-inserts only the affected output partitions, or performs a deterministic upsert on the output primary key |
| Idempotency | Yes. Deduplication is deterministic, surrogate keys are deterministic, and output partitions are replaced atomically for the same batch window. Running the same batch twice produces the same rows and no duplicates. `python -m transformation --force` rewrites the affected partitions; without `--force` an already-successful batch is left intact |

Airflow halts the transformation before writing if the input handover decision is
`REJECTED` or `MANUAL REVIEW` without Data Lead approval.

### 1.3 Transformation Scope

| Input | Output | Fields cleaned | Fields standardized | Fields derived / aggregated |
| --- | --- | --- | --- | --- |
| `staging.raw_logs` | `analytics.fact_event_daily`, `analytics.fact_daily_summary`, `analytics.dim_user` | `event_type`, `user_id`, `payload` | `timestamp` to UTC; event type casing; payload to canonical JSON | Event date, event counts by type, unique users, first/last seen |
| `staging.raw_entities` | `analytics.fact_daily_summary`, `analytics.dim_user` | `status`, `user_id` | `created_at` to UTC; status casing | Created date, status counts, unique users |
| `staging.hb_pitc` | `analytics.dim_health_board`, `analytics.dim_drug`, `analytics.dim_prescribed_type`, `analytics.fact_prescribing_monthly` | `HBT`, `DMDCode`, `BNFItemCode`, `BNFItemDescription`, `PrescribedType` | Codes to uppercase, text trimming, `PaidDateMonth` to month-start DATE | Paid month, average cost per item, average cost per quantity |
| `staging.device_classification` | `analytics.dim_device_classification` | `product_code`, `device_name`, `definition` | Codes uppercase, Y/N flags to Boolean, `last_updated` to DATE | Device class label, SCD1 current row |

Business exclusions are documented in Section 5. Technical filters (batch window
and deduplication) are applied first; excluded business records are counted and
logged, never silently dropped.

### 1.4 Dependencies and Assumptions

Processing order:

1. `dim_date`
2. `dim_user`
3. `dim_health_board`, `dim_drug`, `dim_prescribed_type`, `dim_device_classification`
4. `fact_prescribing_monthly`
5. `fact_event_daily`, then `fact_daily_summary`

Assumptions:

- Stage 1 output has already passed V1–V7 and is `APPROVED` or
  `APPROVED WITH WARNING`.
- All Stage 1 timestamps are already normalized to UTC; Stage 2 still enforces
  UTC and ISO 8601 at the contract boundary.
- The 3-day extraction look-back means the same `log_id` or `entity_id` may
  appear in more than one `business_date` partition. Stage 2 must deduplicate
  before calculating daily facts.
- HB PITC rows with null `DMDCode` / `BNFItemCode` are valid aggregate rows and
  must not be rejected.
- The device classification API returns a variable-size snapshot depending on
  `API_SEARCH`; the current `.env` search (`product_code:NOB`) returns 1 row,
  while a no-filter run can return up to the configured API limit.
- Reference lookup tables are not provided for health-board names or drug
  names beyond the source description. `health_board_name` defaults to the
  health-board code until a reference table is supplied. Unknown or aggregate
  members use the unknown-member pattern rather than failing the batch.
- Late-arriving source records are handled by re-reading the trailing window,
  not by mutating historical extraction files.

---

## 2. Data Profiling and Issue Identification

Profiling was performed with Polars directly on the current staging partitions:
`raw_logs` and `raw_entities` at `business_date=2026-10-06`, `hb_pitc` at
`business_date=2026-07-01`, and `device_classification` at
`business_date=2026-10-27`.

### 2.1 Profile Snapshot

| Table | Rows | Distinct natural keys | Notes |
| --- | ---: | ---: | --- |
| `raw_logs` | 525 | 525 `log_id` | 5 event types; timestamps 2026-10-05 02:33 to 2026-10-06 23:59 UTC |
| `raw_entities` | 200 | 200 `entity_id` | statuses: PENDING 60, COMPLETED 54, CANCELLED 46, ACTIVE 40 |
| `hb_pitc` | 122,141 | 122,141 composite keys | 16 HBT values; one paid month (2026-07) |
| `device_classification` | 1 | 1 `product_code` | current `product_code:NOB` search; scales with API settings |

### 2.2 Issues Found

| # | Table | Column | Issue type | Example | Affected records | Planned treatment |
| --- | --- | --- | --- | --- | ---: | --- |
| DQ-01 | `raw_logs` | `log_id` | Duplicate risk across overlapping extraction partitions | Same `log_id` can appear in `business_date=2026-10-05` and `2026-10-06` because of the 3-day look-back | 0 in current single partition; structural risk on multi-day windows | Deduplicate by `log_id`, keep latest `extracted_at` (C-01) |
| DQ-02 | `raw_logs` | `timestamp` | Late-arriving events | Event timestamp 2026-10-05 02:33 in a 2026-10-06 partition | 525 rows read in the look-back window | Assign `event_date` from `timestamp`, recompute affected dates (B-08) |
| DQ-03 | `raw_logs` | `payload` | Potential invalid JSON | `{"action":"view"}` vs malformed text | 0/525 current | Replace unparseable payload with `{}` and log modified count (C-05) |
| DQ-04 | `raw_logs` | `event_type` | Inconsistent casing / whitespace | `View`, ` view ` | 0 current sample; 5 valid values after normalization | Trim and lowercase (C-04, S-02) |
| DQ-05 | `raw_logs` | `user_id` | Missing value risk | Null or blank user | 0 current | Fill `UNKNOWN_USER` and flag (C-03) |
| DQ-06 | `raw_entities` | `entity_id` | Duplicate risk across overlapping partitions | Same entity in two business-date partitions | 0 current single partition | Deduplicate, keep latest `extracted_at` (C-01) |
| DQ-07 | `raw_entities` | `status` | Inconsistent casing | `Completed`, `CANCELLED` | 0 current; 4 observed values | Trim and uppercase to controlled set (C-04, S-02) |
| DQ-08 | `hb_pitc` | `DMDCode` | Missing value by design (aggregate row) | `NULL` for aggregate VMP/VMPP/AMP row | 38 (0.03%) | Retain; map to aggregate drug member (C-06) |
| DQ-09 | `hb_pitc` | `BNFItemCode`, `BNFItemDescription` | Missing value by design (aggregate row) | `NULL`, `NULL` | 1,597 (1.31%) | Retain; map to aggregate drug member, do not reject (C-06) |
| DQ-10 | `hb_pitc` | `PaidQuantity` | Zero quantity with positive item count | `NumberOfPaidItems=40`, `PaidQuantity=0.0`, cost 0 | 13 (0.01%) | Retain and flag `zero_quantity_flag`; do not reject (C-08) |
| DQ-11 | `hb_pitc` | `HBT` | Non-standard health-board code | `SB0801`, `SB0806` instead of `S08xxxxxx` | 111 (0.09%) | Uppercase and validate; map unknown to `UNKNOWN` member, warning (C-09, S-08) |
| DQ-12 | `hb_pitc` | measures | Negative values risk | Negative paid quantity or cost | 0 current | Reject to quarantine if found (C-07) |
| DQ-13 | `device_classification` | snapshot | Duplicate snapshot versions | Same `product_code` in repeated monthly partitions | 0 current | Keep latest `extracted_at` / `last_updated` (C-10) |
| DQ-14 | `device_classification` | optional attributes | Empty text for missing values | `definition = ""` | 0 current sample | Retain empty text; flags converted to Boolean (S-06) |
| DQ-15 | all tables | schema | Unexpected column rename or type change | Missing `PaidQuantity` or changed `timestamp` type | 0 current | Fail contract test before transform (Section 7) |

---

## 3. Data Cleaning Rules

Every removed or rejected record is written to the quarantine store with
`run_id`, `batch_id`, `rule_id`, and `rejection_reason`. Cleaned and retained
records are counted under `rules_applied` but are not rejected.

| Rule ID | Rule name | Target | Condition | Action | Justification |
| --- | --- | --- | --- | --- | --- |
| C-01 | Overlap deduplication | `raw_logs.log_id`, `raw_entities.entity_id`, `hb_pitc` composite key, `device_classification.product_code` | Same natural key appears more than once across partitions | Keep the row with the greatest `extracted_at`; tie-break by greatest `run_id`; count removed rows as `records_deduplicated` | The extraction look-back intentionally re-reads recent data; keeping the newest extraction preserves the most complete record without double counting |
| C-02 | Null primary key rejection | All input tables | Any primary-key component is null where the extraction contract does not allow it | Quarantine the row with `rule_id=C-02` | A record that cannot be identified cannot be safely joined or deduplicated |
| C-03 | Missing user handling | `raw_logs.user_id`, `raw_entities.user_id` | Null, empty, or whitespace-only value | Fill with `UNKNOWN_USER`, set `user_is_unknown=true`, count as modified | Preserves event/entity volume without inventing a real user; retains auditability |
| C-04 | Whitespace normalization | All string columns | Leading/trailing whitespace, repeated internal spaces in descriptions | Trim ends; collapse repeated spaces to one in description fields | Prevents false join mismatches and inconsistent dimension members |
| C-05 | Payload canonicalization | `raw_logs.payload` | Payload is not valid JSON or is an empty string | Replace with `{}`; count modified; retain original in a quarantined audit copy only if policy allows | The fact tables do not depend on payload content; a malformed payload must not drop a valid event |
| C-06 | Aggregate-row preservation | `hb_pitc.DMDCode`, `BNFItemCode`, `BNFItemDescription` | Null codes on aggregate rows | Retain row; assign `drug_key` of the `AGGREGATE` dimension member; `is_aggregate=true` | Null codes are expected for aggregate prescribing rows and are analytically required |
| C-07 | Invalid measure rejection | `hb_pitc.NumberOfPaidItems`, `PaidQuantity`, `GrossIngredientCost` | Value is negative or non-numeric after conversion | Quarantine the row with the affected rule/column | Negative paid items, quantity, or cost are invalid business values |
| C-08 | Zero-quantity handling | `hb_pitc.PaidQuantity` with `NumberOfPaidItems > 0` | Quantity equals zero but paid items are positive | Retain; set `zero_quantity_flag=true`; count as modified | Zero-cost/zero-quantity reimbursements can be legitimate; rejecting them would understate item counts |
| C-09 | Unknown health board | `hb_pitc.HBT` | Code is blank or not in the accepted reference list | Uppercase; map unknown code to `UNKNOWN` board member; retain row; warning | Preserves national totals while isolating unmapped codes for data-owner review |
| C-10 | Snapshot deduplication | `device_classification.product_code` | Multiple API snapshots for the same product code | Keep the row with the greatest `extracted_at` / `last_updated` | Device classification is a reference snapshot; only the newest version should be current |
| C-11 | Orphan fact protection | Fact-to-dimension joins | A fact row has no matching dimension member | Assign the `UNKNOWN`/`AGGREGATE` member key; count as modified, not rejected | Referential integrity is preserved and the fact remains reportable |
| C-12 | Duplicate entity status | `raw_entities.entity_id` | Same entity appears with different status across time partitions | Keep latest by `extracted_at` for the current entity snapshot; daily counts use the deduplicated event-date view | Entity status is a current-state attribute; the latest extraction is authoritative |

---

## 4. Standardization and Data Type Rules

| Rule ID | Column(s) | Source format | Standard format | On failure |
| --- | --- | --- | --- | --- |
| S-01 | `timestamp`, `created_at`, `extracted_at`, `PaidDateMonth`, `last_updated` | ISO strings, date strings, `YYYYMM`, timestamp with/without timezone | `TIMESTAMP` in UTC, ISO 8601 for external display; month-start `DATE` for paid month | Reject row or quarantine with `rule_id=S-01` |
| S-02 | `event_type`, `status`, `PrescribedType` | Mixed case / surrounding whitespace | Lowercase `event_type`; uppercase `status` and `PrescribedType` | Quarantine if not in the controlled value set |
| S-03 | `HBT`, `DMDCode`, `BNFItemCode`, `product_code`, `device_class` | Numeric-looking or mixed-case codes | Preserve as text, trim, uppercase; never cast a code to a number that would lose leading zeros | Quarantine with `rule_id=S-03` |
| S-04 | `BNFItemDescription`, `device_name`, `definition` | Free text | Trimmed, whitespace-collapsed text; retain original case and punctuation | Keep text; no failure |
| S-05 | `NumberOfPaidItems` | Integer / numeric text | `INTEGER` (32-bit safe) | Reject with `rule_id=S-05` |
| S-06 | `PaidQuantity`, `GrossIngredientCost` | Float / numeric text | `DECIMAL(18,4)` for quantity; `DECIMAL(18,2)` for GBP cost | Reject with `rule_id=S-06` |
| S-07 | `implant_flag`, `life_sustain_support_flag`, `gmp_exempt_flag`, `third_party_flag` | `Y` / `N` strings | `BOOLEAN` (`Y=true`, `N=false`) | Set `NULL` and warning, because these are optional attributes |
| S-08 | `HBT` | `S08xxxxxx`, `SBxxxx` | Uppercase, trimmed, validated against accepted health-board codes | Map to `UNKNOWN` member and warning |
| S-09 | `device_class` | `1`, `2`, `3`, `U`, `N` strings | Controlled text: `CLASS_1`, `CLASS_2`, `CLASS_3`, `UNCLASSIFIED`, `NOT_CLASSIFIED` | Keep raw value and flag unknown classification |
| S-10 | `user_id`, `entity_id`, `log_id` | UUID/text | Trimmed text; preserve original identifier | Quarantine null primary keys; fill unknown user only for user references |

Numeric rounding is applied only at the output boundary. `PaidQuantity` is
rounded half-up to 4 decimal places and `GrossIngredientCost` to 2 decimal
places. Derived averages are computed from unrounded staging values and then
rounded to 4 decimal places.

---

## 5. Business Transformation Rules

| Rule ID | Input columns | Logic / formula | Output | Business reason |
| --- | --- | --- | --- | --- |
| B-01 | `timestamp`, `created_at`, `PaidDateMonth`, `last_updated` | Derive calendar date and month-start period; build `dim_date` | `event_date`, `created_date`, `paid_month`, `date_key`, `year`, `quarter`, `month`, `week`, `is_weekend` | Provides consistent date dimensions for all reporting |
| B-02 | `raw_logs.user_id`, `raw_entities.user_id`, `timestamp`, `created_at` | `first_seen_at = min(timestamp/created_at)`, `last_seen_at = max(...)` per user | `analytics.dim_user` | Enables unique-user measures and user activity lineage |
| B-03 | `raw_entities.status` | Count entities by `created_date` and status | `entities_created`, `entities_active`, `entities_completed`, `entities_cancelled` in `fact_daily_summary` | Status counts are required for operational reporting |
| B-04 | `raw_logs.event_type`, `log_id` | `event_count = count(distinct log_id)` per `event_date` and `event_type` | `analytics.fact_event_daily` | Flexible daily event reporting without hard-coding event categories |
| B-05 | `fact_event_daily`, `dim_user` | Aggregate `event_count` and `unique_users` per `event_date`; pivot known event types into named measures | `analytics.fact_daily_summary` | Produces the target daily data mart measure set |
| B-06 | `hb_pitc.HBT` | Create one dimension row per accepted board code; `health_board_name = health_board_code` until a reference table is available; add `UNKNOWN` member | `analytics.dim_health_board` | Health-board reporting and referential integrity |
| B-07 | `hb_pitc.DMDCode`, `BNFItemCode`, `BNFItemDescription` | Create one dimension row per unique `(DMDCode, BNFItemCode, description)`; add `AGGREGATE` member for null-code rows | `analytics.dim_drug` | Drug-level reporting while retaining aggregate PITC rows |
| B-08 | `hb_pitc.PrescribedType` | Create one row per controlled prescribed type | `analytics.dim_prescribed_type` | Consistent AMP/VMP/AMPP/VMPP reporting |
| B-09 | `hb_pitc` measures | Grain: `(health_board_key, drug_key, prescribed_type_key, paid_month)`; measures: `paid_items`, `paid_quantity`, `gross_ingredient_cost`; derived `avg_cost_per_item = gross_ingredient_cost / nullif(paid_items, 0)`, `avg_cost_per_quantity = gross_ingredient_cost / nullif(paid_quantity, 0)` | `analytics.fact_prescribing_monthly` | Monthly prescribing cost and volume analysis |
| B-10 | `device_classification` attributes | Keep one current row per `product_code`; overwrite changed attributes (SCD1); flags Y/N to Boolean; classification label mapped | `analytics.dim_device_classification` | Stable device reference for downstream device-related reporting |
| B-11 | `hb_pitc` | `zero_quantity_flag = (paid_quantity = 0 and paid_items > 0)` | `fact_prescribing_monthly.zero_quantity_flag` | Makes the 13 observed zero-quantity rows visible without rejecting them |
| B-12 | Fact loads | Business exclusion: no log/entity records are excluded from `fact_daily_summary` because cancelled and pending statuses are required measures; exclusions, if introduced later, must be recorded with count and reason | `records_excluded` metric | Prevents silent loss of business-relevant statuses |
| B-13 | Surrogate keys | Generate deterministic integer keys using a hash of the natural key and a stable key namespace | `*_key` columns | Enables stable joins across reruns and avoids depending on row order |

Join policy:

| Join | Keys | Type | Unmatched handling |
| --- | --- | --- | --- |
| `fact_prescribing_monthly` → `dim_health_board` | `health_board_key` | Inner after unknown-member assignment | Use `UNKNOWN` member key; warning count incremented |
| `fact_prescribing_monthly` → `dim_drug` | `drug_key` | Inner after aggregate/unknown assignment | Use `AGGREGATE` member key |
| `fact_prescribing_monthly` → `dim_prescribed_type` | `prescribed_type_key` | Inner | Quarantine if type is not in controlled set |
| `fact_event_daily` → `dim_date` | `event_date_key` | Inner | Quarantine if date is outside the valid calendar |
| `fact_event_daily` → `dim_user` | `user_key` | Inner after `UNKNOWN_USER` assignment | Use `UNKNOWN_USER` key |
| `fact_daily_summary` → `dim_date` | `event_date_key` | Inner | One row per date; no unmatched expected |

All lookup dimensions are deduplicated on their natural key before joins. Join
cardinality must be many-to-one; any one-to-many lookup is a critical validation
failure.

---

## 6. Transformed Data Contract and Naming Convention

### 6.1 Naming Convention

- Schema: `analytics`
- Tables: `dim_` for dimensions, `fact_` for facts
- Columns: `snake_case`
- Natural identifiers: `_id` or `_code` (for example `user_id`, `health_board_code`)
- Surrogate keys: `_key` (for example `user_key`, `drug_key`)
- Timestamps: `_at`; dates: `_date`; flags: `is_` / `_flag`
- Run lineage: `transformed_run_id`; the parent extraction run is recorded in
  transformation metadata as `parent_run_id`
- Output files: `data/transformed/<table>/business_date=YYYY-MM-DD/data.parquet`
  for daily facts, `business_date=YYYY-MM-01/data.parquet` for monthly facts and
  dimensions
- Handover manifest:
  `data/transformed/_handover/business_date=YYYY-MM-DD/manifest.json`

### 6.2 Required Output Schema

#### `analytics.dim_date`

| Column | Type | Null | Key |
| --- | --- | --- | --- |
| `date_key` | INTEGER | No | PK |
| `calendar_date` | DATE | No | Unique |
| `year` | INTEGER | No | |
| `quarter` | INTEGER | No | |
| `month` | INTEGER | No | |
| `day` | INTEGER | No | |
| `week_of_year` | INTEGER | No | |
| `is_weekend` | BOOLEAN | No | |

#### `analytics.dim_user`

| Column | Type | Null | Key |
| --- | --- | --- | --- |
| `user_key` | BIGINT | No | PK |
| `user_id` | STRING | No | Unique natural key |
| `first_seen_at` | TIMESTAMP (UTC) | No | |
| `last_seen_at` | TIMESTAMP (UTC) | No | |
| `user_is_unknown` | BOOLEAN | No | |
| `updated_at` | TIMESTAMP (UTC) | No | |

#### `analytics.dim_health_board`

| Column | Type | Null | Key |
| --- | --- | --- | --- |
| `health_board_key` | INTEGER | No | PK |
| `health_board_code` | STRING | No | Unique natural key |
| `health_board_name` | STRING | No | |
| `is_unknown` | BOOLEAN | No | |

#### `analytics.dim_drug`

| Column | Type | Null | Key |
| --- | --- | --- | --- |
| `drug_key` | BIGINT | No | PK |
| `dmd_code` | BIGINT | Yes | Part of natural key |
| `bnf_item_code` | STRING | Yes | Part of natural key |
| `bnf_item_description` | STRING | Yes | |
| `is_aggregate` | BOOLEAN | No | |
| `updated_at` | TIMESTAMP (UTC) | No | |

#### `analytics.dim_prescribed_type`

| Column | Type | Null | Key |
| --- | --- | --- | --- |
| `prescribed_type_key` | INTEGER | No | PK |
| `prescribed_type_code` | STRING | No | Unique natural key |
| `prescribed_type_description` | STRING | No | |

#### `analytics.dim_device_classification`

| Column | Type | Null | Key |
| --- | --- | --- | --- |
| `device_key` | BIGINT | No | PK |
| `product_code` | STRING | No | Unique natural key |
| `device_name` | STRING | No | |
| `device_class` | STRING | No | |
| `regulation_number` | STRING | Yes | |
| `medical_specialty_description` | STRING | Yes | |
| `review_panel` | STRING | Yes | |
| `implant_flag` | BOOLEAN | Yes | |
| `life_sustain_support_flag` | BOOLEAN | Yes | |
| `gmp_exempt_flag` | BOOLEAN | Yes | |
| `third_party_flag` | BOOLEAN | Yes | |
| `submission_type_id` | STRING | Yes | |
| `definition` | STRING | Yes | |
| `last_updated` | DATE | No | |
| `updated_at` | TIMESTAMP (UTC) | No | |

#### `analytics.fact_prescribing_monthly`

| Column | Type | Null | Key |
| --- | --- | --- | --- |
| `prescribing_key` | BIGINT | No | PK |
| `health_board_key` | INTEGER | No | FK → `dim_health_board` |
| `drug_key` | BIGINT | No | FK → `dim_drug` |
| `prescribed_type_key` | INTEGER | No | FK → `dim_prescribed_type` |
| `period_key` | INTEGER | No | FK → `dim_date` |
| `paid_month` | DATE | No | Month start |
| `paid_items` | INTEGER | No | ≥ 0 |
| `paid_quantity` | DECIMAL(18,4) | No | ≥ 0 |
| `gross_ingredient_cost` | DECIMAL(18,2) | No | ≥ 0 |
| `avg_cost_per_item` | DECIMAL(18,4) | Yes | Null when `paid_items = 0` |
| `avg_cost_per_quantity` | DECIMAL(18,4) | Yes | Null when `paid_quantity = 0` |
| `zero_quantity_flag` | BOOLEAN | No | |
| `transformed_run_id` | STRING | No | Lineage to transformation run |
| `updated_at` | TIMESTAMP (UTC) | No | |

Unique natural key: `(health_board_key, drug_key, prescribed_type_key, period_key)`.

#### `analytics.fact_event_daily`

| Column | Type | Null | Key |
| --- | --- | --- | --- |
| `event_fact_key` | BIGINT | No | PK |
| `event_date_key` | INTEGER | No | FK → `dim_date` |
| `user_key` | BIGINT | No | FK → `dim_user` |
| `event_type` | STRING | No | |
| `event_count` | BIGINT | No | ≥ 0 |
| `transformed_run_id` | STRING | No | |

Unique natural key: `(event_date_key, user_key, event_type)`.

#### `analytics.fact_daily_summary`

| Column | Type | Null | Key |
| --- | --- | --- | --- |
| `summary_key` | BIGINT | No | PK |
| `event_date_key` | INTEGER | No | FK → `dim_date`, unique |
| `total_events` | BIGINT | No | ≥ 0 |
| `unique_users` | BIGINT | No | ≥ 0 |
| `signup_count` | BIGINT | No | ≥ 0 |
| `purchase_count` | BIGINT | No | ≥ 0 |
| `view_count` | BIGINT | No | ≥ 0 |
| `click_count` | BIGINT | No | ≥ 0 |
| `logout_count` | BIGINT | No | ≥ 0 |
| `entities_created` | BIGINT | No | ≥ 0 |
| `entities_active` | BIGINT | No | ≥ 0 |
| `entities_completed` | BIGINT | No | ≥ 0 |
| `entities_cancelled` | BIGINT | No | ≥ 0 |
| `transformed_run_id` | STRING | No | |

### 6.3 Source-to-Target Mapping

| Source | Source column | Target column | Source type | Target type | Transformation |
| --- | --- | --- | --- | --- | --- |
| `raw_logs` | `log_id` | `fact_event_daily.event_count` | STRING | BIGINT | Count distinct after C-01 |
| `raw_logs` | `event_type` | `fact_event_daily.event_type` | STRING | STRING | Trim + lowercase (C-04, S-02) |
| `raw_logs` | `timestamp` | `fact_event_daily.event_date_key` | TIMESTAMP (UTC) | INTEGER | Derive date, join `dim_date` (B-01) |
| `raw_logs` | `user_id` | `fact_event_daily.user_key` | STRING | BIGINT | Fill unknown (C-03), lookup `dim_user` (B-02) |
| `raw_logs` | `log_id` | `fact_daily_summary.total_events` | STRING | BIGINT | Count distinct across event types (B-05) |
| `raw_logs` | `user_id` | `fact_daily_summary.unique_users` | STRING | BIGINT | Count distinct after unknown handling (B-05) |
| `raw_entities` | `created_at` | `fact_daily_summary.event_date_key` | TIMESTAMP (UTC) | INTEGER | Derive date (B-01) |
| `raw_entities` | `status` | `entities_*` measures | STRING | BIGINT | Conditional counts (B-03) |
| `hb_pitc` | `HBT` | `dim_health_board.health_board_code` | STRING | STRING | Uppercase, validate (C-09, S-08) |
| `hb_pitc` | `DMDCode`, `BNFItemCode` | `dim_drug` natural key | INTEGER, STRING | BIGINT, STRING | Preserve codes; null → `AGGREGATE` (C-06, B-07) |
| `hb_pitc` | `PaidDateMonth` | `fact_prescribing_monthly.paid_month` | TIMESTAMP (UTC) | DATE | Month-start date (S-01) |
| `hb_pitc` | `NumberOfPaidItems` | `fact_prescribing_monthly.paid_items` | INTEGER | INTEGER | Cast and validate (S-05) |
| `hb_pitc` | `PaidQuantity` | `fact_prescribing_monthly.paid_quantity` | FLOAT | DECIMAL(18,4) | Round half-up (S-06) |
| `hb_pitc` | `GrossIngredientCost` | `fact_prescribing_monthly.gross_ingredient_cost` | FLOAT | DECIMAL(18,2) | Round half-up (S-06) |
| `hb_pitc` | `GrossIngredientCost`, `NumberOfPaidItems` | `avg_cost_per_item` | FLOAT, INTEGER | DECIMAL(18,4) | Derived (B-09) |
| `device_classification` | `product_code` | `dim_device_classification.product_code` | STRING | STRING | Uppercase + deduplicate (C-10, S-03) |
| `device_classification` | flags | `*_flag` | STRING | BOOLEAN | Y/N conversion (S-07) |
| `device_classification` | `last_updated` | `dim_device_classification.last_updated` | TIMESTAMP (UTC) | DATE | Date cast (S-01) |

### 6.4 Schema Consistency

Before any write, the transformation engine's schema checks and contract tests
verify that every contracted source column is present and has a compatible type.
The batch fails if a required column is missing, renamed, or its type changes
incompatibly. New nullable source columns
are ignored until the contract version is updated. Contract changes require a
new rules version in `transformation_version` and a coordinated Load-stage
release.

---

## 7. Transformation Validation and Data Quality Checks

Reconciliation equation used for row-level outputs:

```text
records_read = records_output
             + records_rejected
             + records_deduplicated
             + records_excluded
```

Cleaned-and-retained rows are `records_modified` and are reported through
`rules_applied`; they are not part of the reconciliation equation.

| Check ID | Check name | Target | Purpose | Validation criteria | Severity |
| --- | --- | --- | --- | --- | --- |
| TV-01 | Record reconciliation | All transformed tables | Detect lost or duplicated records | `records_read = records_output + records_rejected + records_deduplicated + records_excluded` | Critical |
| TV-02 | Primary-key uniqueness | All output tables | Prevent duplicate fact/dimension rows | Duplicate count on declared PK = 0 | Critical |
| TV-03 | Required-column null check | `dim_*` and `fact_*` required columns | Ensure Load receives complete keys and measures | Null count = 0 for all `NOT NULL` contract columns | Critical |
| TV-04 | Referential integrity | All fact foreign keys | Prevent orphan facts | 100% of fact FK values exist in their dimension; unmatched rows = 0 or mapped to unknown member | Critical |
| TV-05 | Join cardinality | Dimension lookups before fact load | Prevent row multiplication | Each lookup natural key is unique; fact row count after join ≤ deduplicated input row count | Critical |
| TV-06 | Type and format conformance | All contracted columns | Guarantee Load-compatible data | 100% of values conform to Section 4 types, UTC timestamps, ISO dates, decimal scales | Critical |
| TV-07 | Business-rule checks | PITC measures and daily measures | Prevent invalid business values | No negative `paid_items`, `paid_quantity`, `gross_ingredient_cost`, event counts, or unique-user counts | Critical |
| TV-08 | Aggregate reconciliation | `fact_prescribing_monthly` | Ensure measures are not lost during cleaning | Sum of `paid_items`, `paid_quantity`, and `gross_ingredient_cost` after transformation equals the sum of accepted input rows within 0.01 tolerance for costs and quantities | Critical |
| TV-09 | Event reconciliation | `fact_event_daily`, `fact_daily_summary` | Ensure daily counts match deduplicated logs | Output event count equals deduplicated input `log_id` count for each date; summary total equals sum of event rows | Critical |
| TV-10 | Quarantine completeness | Quarantine store | Ensure every rejected record is traceable | `records_rejected` equals number of quarantine rows for the batch; every row has `rule_id` and `rejection_reason` | Critical |
| TV-11 | SCD/dimension integrity | `dim_device_classification`, `dim_drug` | Prevent duplicate current members | Exactly one current row per natural key | Critical |
| TV-12 | Zero-quantity visibility | `fact_prescribing_monthly.zero_quantity_flag` | Ensure observed zero-quantity rows are retained and flagged | Count of flagged rows equals accepted input zero-quantity rows (13 in the profiled month) | Non-critical |
| TV-13 | Unknown-member rate | Unknown user/board/drug members | Detect unexpected mapping loss | Unknown-member share per dimension ≤ 1% of rows; above 1% raises warning | Non-critical |
| TV-14 | Freshness | All output partitions | Prevent stale handover | Maximum `updated_at` is within the batch window and not older than the extraction `extracted_at` | Non-critical |

Extraction-stage checks are not repeated. Extraction connectivity, source
completeness, and V1–V7 results are referenced through extraction metadata for
reconciliation only.

---

## 8. Transformation Metadata and Log Specification

Transformation metadata is stored in
`data/metadata/sightsync_transform_audit.db` (`transformation_runs` table).
Each run also writes `data/logs/transformation/<business_date>/<run_id>.json`.

| Field | Purpose | Example |
| --- | --- | --- |
| `run_id` | Unique transformation run ID | `TRF_20261027_0200_a1b2c3` |
| `parent_run_id` | Extraction run that produced the input | `a8f3b1e2-9c10-4e88-b223-112233445566` |
| `batch_id` | Idempotency key for the batch window | `sightsync_transform:2026-10-27` |
| `batch_window_start` | First business date processed | `2026-10-24` |
| `batch_window_end` | Last business date processed | `2026-10-27` |
| `table_name` | Output table or file | `analytics.fact_daily_summary` |
| `transformation_started_at` | UTC start timestamp | `2026-10-27T02:05:00Z` |
| `transformation_ended_at` | UTC end timestamp | `2026-10-27T02:05:42Z` |
| `duration_ms` | Wall-clock duration | `42000` |
| `status` | Run result | `SUCCESS`, `FAILED`, `QUARANTINED` |
| `records_read` | Rows received from staging | `525` |
| `records_output` | Rows produced | `520` |
| `records_rejected` | Rows sent to quarantine | `2` |
| `records_deduplicated` | Overlapping rows removed | `3` |
| `records_excluded` | Business exclusions | `0` |
| `records_modified` | Rows retained but changed | `525` |
| `rules_applied` | Per-rule affected counts (JSON) | `{"C-01":3,"C-03":0,"S-02":525}` |
| `validation_results` | TV check statuses (JSON) | `{"TV-01":"PASS","TV-02":"PASS"}` |
| `error_code` | Stable machine-readable failure code | `TV-04_REFERENTIAL_INTEGRITY` |
| `error_message` | Human-readable failure detail | `fact_prescribing_monthly has 4 orphan drug_key values` |
| `transformation_version` | Code/rules contract version | `2.0.0` |

Usage:

- **Monitoring:** Airflow reads `status`, `duration_ms`, and `validation_results`
  to alert on failures or slow batches.
- **Troubleshooting:** `error_code`, `error_message`, and `parent_run_id` lead
  directly to the affected extraction run and rule.
- **Auditing:** `rules_applied`, `records_modified`, and validation history prove
  what changed and why.
- **Lineage:** `parent_run_id` + output `transformed_run_id` trace every output
  row back to its extraction batch.
- **Recovery:** the `batch_id` and batch window allow a failed run to be
  restarted idempotently without deleting unaffected partitions.

---

## 9. Log Retention, Rejected Data, and Access

| Item | Location | Retention | Justification |
| --- | --- | --- | --- |
| Transformation operational logs | `data/logs/transformation/<business_date>/` | 90 days | Supports routine troubleshooting and batch recovery |
| Transformation audit metadata | `data/metadata/sightsync_transform_audit.db` | 24 months | Matches the Stage 1 audit retention and supports historical verification |
| Quarantine (rejected records) | `data/quarantine/<table>/business_date=YYYY-MM-DD/rejected.parquet` | 24 months | Rejected data must remain available for audit, rework, and dispute resolution |
| Validation results | Stored in the audit database and run JSON log | 24 months with audit records | Demonstrates that handover decisions were rule-based |

Quarantine columns:

- All rejected source columns needed for rework
- `run_id`, `batch_id`, `parent_run_id`
- `rule_id`, `rejection_reason`, `rejected_at`
- `source_table`, `business_date`

Access permissions:

- **Write:** Airflow transformation service account / Data Engineering
- **Read:** Data Engineering, Data Lead, internal audit
- **Delete:** only the scheduled retention job, with Data Lead approval
- **Quarantine read:** Data Engineering and Data Lead; access is restricted
  because records may contain user identifiers or payload fragments

No transformation log or rejected record may be deleted before its retention
period ends or while it is required for an open incident. Logs must not duplicate
full payloads or sensitive source values; store identifiers and rejection
reasons only. After 90 days, operational JSON logs may be archived to compressed
cold storage for the remainder of the 24-month audit period.

---

## 10. Transformation Acceptance and Handover Rules

The project rejection-rate threshold is **1% of records read**, chosen in advance
to match the Stage 1 acceptance threshold. It is strict enough to block a batch
with material data loss but allows for the small expected rate of duplicate and
late-arriving records created by the 3-day look-back.

| Decision | Overall condition | Pipeline action | Automatic / manual | Responsible role |
| --- | --- | --- | --- | --- |
| `APPROVED` | All critical TV checks PASS; no non-critical warnings; `records_rejected / records_read ≤ 1%` | Publish transformed Parquet, metadata, and validation results to the Load handover location | Automatic | Airflow transformation service |
| `MANUAL REVIEW` | All critical TV checks PASS, but one or more non-critical warnings OR `1% < rejection rate ≤ 5%` | Hold the batch; do not hand over until reviewed and released | Manual | Data Lead |
| `REJECTED` | Any critical TV check FAILS OR rejection rate > 5% | Stop the batch; retry up to 3 times; if still failing, quarantine the batch and raise an engineer alert | Automatic stop, manual remediation | Data Engineering |

Retry and recovery:

- Airflow retries a failed batch up to 3 times with a 10-minute delay.
- Retries rerun the same batch window and use the same idempotent write path.
- If the failure is caused by a critical validation check, retries do not bypass
  the check; the batch remains quarantined until the cause is corrected.
- A corrected batch must use a new `run_id` but keeps the same `batch_id` so
  downstream duplicate detection remains valid.

Handover to Load:

- Partitioned Parquet output for every `analytics.dim_*` and `analytics.fact_*`
  table listed in Section 6.
- Handover manifest at
  `data/transformed/_handover/business_date=YYYY-MM-DD/manifest.json` with the
  run lineage, table counts, `validation_results`, and quarantine counts by
  `rule_id`.
- Transformation metadata row(s) with `run_id`, `parent_run_id`, `batch_id`,
  counts, `rules_applied`, and `validation_results`.
- Quarantine manifest containing rejection counts by `rule_id`.
- Transformation contract version (`transformation_version`) and transformation
  validation results.
- Extraction handover reference (`parent_run_id`) so Load can trace data back to
  the exact Stage 1 run.

Example validation summary:

| Result | Status |
| --- | --- |
| `dim_date` | PASS |
| `dim_user` | PASS |
| `dim_health_board` | WARNING (unknown board rate 0.09%) |
| `dim_drug` | PASS |
| `dim_device_classification` | PASS |
| `fact_prescribing_monthly` | PASS |
| `fact_event_daily` | PASS |
| `fact_daily_summary` | PASS |
| Overall transformation | MANUAL REVIEW (non-critical warning) |

---

## 11. Common Transformation Problems and Mitigation

| Problem | Possible cause | Impact on transformation | Mitigation |
| --- | --- | --- | --- |
| Duplicate records after a rerun | Look-back overlap or non-idempotent write | Inflated event counts and PITC measures | C-01 deterministic deduplication; delete-and-insert the affected output partition by `batch_id` |
| Late-arriving records | Source posts after the extraction window | Missing events or stale daily summaries | Re-read the 3-day trailing staging window and recompute every affected `event_date` |
| Monthly PITC correction arrives late | NHS file corrected after month close | Wrong monthly totals | Re-read current and previous month; recompute both `fact_prescribing_monthly` partitions |
| Row multiplication after joins | Duplicate natural keys in `dim_drug` or `dim_health_board` | Inflated measures and output counts above input | Deduplicate dimensions and enforce one-to-many join cardinality (TV-05) |
| Null DMD/BNF codes create an oversized dimension | Every null combination treated as a distinct drug | Dimension explosion and unstable surrogate keys | Use one `AGGREGATE` member for null-code rows (C-06, B-07) |
| Unknown HBT codes | New board code or non-standard source value | Orphan fact rows or silently dropped rows | Map to `UNKNOWN` board member, retain the row, and report the unknown rate (C-09, TV-13) |
| Unexpected schema change in staging | Source column renamed or type changed | Transformation job fails or silently misreads data | Contract/schema tests run before transformation and fail the batch (Section 7) |
| Duplicate device snapshots | API refreshed multiple times per month | Multiple current rows per product code | Keep latest snapshot by `extracted_at`/`last_updated`; enforce one current row (C-10, TV-11) |
| API pagination truncation | `API_LIMIT` smaller than the available snapshot | Incomplete device dimension | Compare fetched count with API `meta.results.total`; warn when truncated |
| Skewed PITC aggregation | Uneven PITC rows per health board or drug | Long-running monthly batch | Partition by `HBT`/`PaidDateMonth` and aggregate before joins |

---

## Handover Summary

Stage 2 receives only the approved Stage 1 Parquet handover, applies the
cleaning, standardization, and business rules above, validates the transformed
output against reconciliation and business rules, and publishes the documented
`analytics` dimensional model for the Load stage. Extraction metadata is
referenced through `parent_run_id`; source systems are never read again. Loading
into the target database is explicitly out of scope for this stage.
