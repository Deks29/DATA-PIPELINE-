# Stage 2: Data Transformation Plan
**BAT-405 Analytics Application Capstone — Laboratory Activity 4**

**Authors:** John Patrick Russel D. Lalo, Dexter T. Manalo, Hazel Ann D. Manimtim

## Objective

The purpose of Stage 2 is to convert the verified output from Stage 1 into a reliable, consistent, and analysis-ready dataset that can be passed to the Load stage. Before the data is handed over, the transformation process must be documented, checked against the original staged records, and supported by validation results. The process should also maintain enough information to trace each transformed output back to the extraction run from which it came.

This activity covers data transformation only. It does not connect to source systems to extract the data again, and it does not insert the transformed data into the target data warehouse. Instead, it uses the Parquet files produced by Stage 1, applies the approved cleaning and transformation rules, and saves the resulting datasets as partitioned Parquet files. The schemas and rules described in this plan serve as the agreement that the next stage will follow, so the Load stage will not need to recreate the same business logic.

The central goal is to make the data suitable for analysis without losing the meaning of the original records. Transformation should improve consistency, completeness, and usability while preserving enough source information to explain how each output value was produced. Every adjustment must follow a defined rule rather than an informal decision made during a particular run. This makes the process easier to repeat, test, review, and maintain when future batches contain new or corrected records.

The plan also separates successful processing from successful handover. A job finishing without a software error does not automatically mean its output is acceptable. The records must still satisfy the data-quality controls, and the resulting files must match the agreed schemas. The handover decision is therefore based on both execution status and validation evidence.

---

The specification in this section establishes the controlled starting point for every transformation run. It explains which staged datasets may be read, how the workflow identifies the relevant partitions, and which operational conditions must be satisfied before the data can be processed. Keeping these details explicit reduces the risk that two runs use different inputs while appearing to perform the same task. It also provides a shared reference for developers, reviewers, and the team responsible for loading the final datasets.

A transformation run should be reproducible from its input references and documented rule version. If a later investigation finds a problem in an output, the team should be able to identify the original staging partition, the parent extraction run, the transformation configuration, and the validation results without relying on memory or undocumented manual steps. The run should therefore treat input selection as a controlled operation and record the chosen partitions before any cleaning or aggregation begins.

## 1. Transformation Input and Processing Specification

### 1.1 Input Sources

The transformation workflow will use the Parquet staging files created by the Stage 1 extraction process. Once Stage 1 has completed, Stage 2 will work from those staged files instead of querying the original source systems again. This separation helps keep the extraction and transformation responsibilities clear and makes the transformation results easier to reproduce.

| Input dataset | Staging file location | File format | Natural key or key combination |
|---|---|---|---|
| `staging.raw_logs` | `data/staging/raw_logs/business_date=YYYY-MM-DD/data.parquet` | Parquet using Zstandard compression | `log_id` |
| `staging.raw_entities` | `data/staging/raw_entities/business_date=YYYY-MM-DD/data.parquet` | Parquet using Zstandard compression | `entity_id` |
| `staging.hb_pitc` | `data/staging/hb_pitc/business_date=YYYY-MM-01/data.parquet` | Parquet using Zstandard compression | `HBT`, `DMDCode`, `BNFItemCode`, `PrescribedType`, `PaidDateMonth` |
| `staging.device_classification` | `data/staging/device_classification/business_date=YYYY-MM-DD/data.parquet` | Parquet using Zstandard compression | `product_code` |

The staged records contain control fields such as `run_id`, `business_date`, and `extracted_at`. These fields identify the extraction run, the business date associated with the data, and the time at which the record was extracted. Rather than duplicating all extraction metadata in every transformed record, the transformation run will preserve its relationship to the source run by recording the extraction identifier in `parent_run_id`.

Stage 2 will consult the Stage 1 audit information before processing. The audit information is available in the following locations:

- **Extraction metadata:** `data/metadata/sightsync_audit.db`, specifically the `extraction_runs` table.
- **Operational extraction logs:** `data/logs/<business_date>/<run_id>_<table>.json`.
- **Handover decision:** the extraction handover must be marked `APPROVED` or `APPROVED WITH WARNING`.

For example, if Stage 1 produces the `raw_logs` and `raw_entities` partitions for `business_date=2026-10-06` under an extraction run identified as `EXT_...`, Stage 2 will read those staged partitions and profile their contents. It will then apply the applicable cleaning rules (`C-*`), standardization rules (`S-*`), and business transformation rules (`B-*`). The processed data will be written to the appropriate transformed output for the relevant business date.

Using a known staging location and a documented natural key makes it possible to identify the exact input records involved in a run. The natural key is important when checking whether records have been repeated across partitions or whether a source has provided more than one version of the same logical record. Before transformation begins, the workflow should confirm that the expected files exist, can be read, and correspond to the approved extraction run. If the input does not match the handover information, processing should stop rather than silently selecting a different file.

The batch approach is suitable for this plan because the workflow works with defined business-date and monthly partitions rather than continuously updating each output record as soon as it arrives. A batch gives the team a clear processing window, a measurable set of input records, and a point at which validation can be completed before handover. It also makes it easier to compare record counts between the staged input, accepted output, and quarantine area.

The orchestration layer and the transformation code have separate responsibilities. Airflow coordinates the order of tasks, determines whether prerequisites have completed, applies retry settings, and records task outcomes. Polars performs the actual tabular processing, including filtering, selecting columns, normalizing values, deduplicating records, joining dimensions, and calculating aggregates. Unit tests provide an additional layer of protection by checking that the code follows the documented rules. These responsibilities should remain distinct so that a scheduling change does not unintentionally change business logic.

Before a batch starts, the workflow should identify its business-date range and the partitions that will be read. During execution, it should track the status of each major task rather than recording only one final status. This makes it easier to identify whether a failure occurred while reading input, cleaning records, building dimensions, calculating facts, writing output, or validating the result. A completed task should not be treated as proof that all later tasks have succeeded.

### 1.2 Batch Processing Approach

The transformation job will run as a scheduled batch. Polars will perform the data-processing operations in a standalone Python package named `transformation`, while Airflow will coordinate the workflow, schedule executions, and manage retries. Automated tests in `tests/test_transformation.py` will check that the transformation rules and output contracts are followed.

| Processing item | Planned configuration |
|---|---|
| Transformation engine | Polars batch processing within the standalone `transformation` Python package, coordinated by Airflow. Rule and schema checks will be automated through unit tests. |
| Schedule | Daily at 02:00 UTC, equivalent to 10:00 a.m. in Asia/Manila, after the Stage 1 extraction DAG finishes successfully. |
| Logs and entities | Process the current `business_date` partition together with the three-day trailing look-back already stored in staging. Recalculate every event or creation date affected by the records read. |
| HB PITC data | Read the current month's partition and the preceding month's partition. Rebuild both monthly outputs so that late-arriving corrections can be included. |
| Device classification | Read the newest API snapshot and apply Slowly Changing Dimension Type 1 (SCD Type 1) behavior when `last_updated` or another source attribute has changed. |
| Batch size | One business-date batch for logs and entities, one or two calendar months for PITC data, and one complete API snapshot for device classification. |
| Retry policy | Airflow may retry a failed run up to three times, waiting ten minutes between attempts. |
| Rerun behavior | Replace only the output partitions affected by the batch, or perform a deterministic upsert using the output's declared natural or primary key. |
| Idempotency | Processing the same batch more than once must produce the same final records without introducing duplicates. Deduplication and surrogate-key generation must be deterministic, and partition replacement must be atomic. The command `python -m transformation --force` will rebuild the affected partitions; without `--force`, a batch that already completed successfully will remain unchanged. |

Before any output is written, Airflow must check the Stage 1 handover status. Processing must stop if the input is marked `REJECTED`, or if it is marked `MANUAL REVIEW` and the Data Lead has not approved it. This prevents unapproved or questionable source data from being transformed and passed downstream.

Batch orchestration also provides a consistent sequence for operational tasks. A run should record when it started, which input partitions it selected, which transformation version it used, and whether it completed or failed. These details help distinguish a data-quality issue from an infrastructure or scheduling issue. The retry policy is intended for recoverable execution failures; it must not be used to bypass a failed quality rule or to make an unapproved input appear acceptable.

The scope is designed to make each source useful for analysis while retaining the meaning and traceability of the source information. Cleaning addresses values that are missing, malformed, duplicated, or inconsistent. Standardization applies common formats and data types so that equivalent values are represented consistently. Business transformation creates the dimensions and facts needed for reporting, including date-based summaries and monthly prescribing measures. Each category of work has a different purpose, so the implementation should not combine them into undocumented changes that are difficult to test separately.

The output table selected for a field depends on how that field is used in analysis. Descriptive attributes generally belong in dimensions, while measurable events and summarized quantities belong in fact tables. A field may also be used only during validation or as a join key and therefore may not appear in the final analytical schema. This is expected as long as the mapping and transformation logic are documented and the final output satisfies the contract in Section 6.

Scope control is important when unexpected source values appear. The team should first determine whether the value can be handled by an existing rule, such as mapping a missing reference to an approved unknown member. If the value changes the meaning of a metric or requires a new business definition, the change should be reviewed and documented rather than added informally to the processing code. This keeps the plan consistent across batches and prevents different versions of the same metric from being produced without explanation.

### 1.3 Scope of Transformation

The workflow will clean, standardize, derive, and aggregate fields according to the intended output tables. The table below summarizes the principal work for each source.

| Input dataset | Main output datasets | Cleaning activities | Standardization activities | Derived or summarized information |
|---|---|---|---|---|
| `staging.raw_logs` | `analytics.fact_event_daily`, `analytics.fact_daily_summary`, `analytics.dim_user` | Check `event_type`, `user_id`, and `payload` | Convert timestamps to UTC, normalize event-type casing, and convert payloads to canonical JSON | Event date, event totals by type, distinct users, and first/last activity times |
| `staging.raw_entities` | `analytics.fact_daily_summary`, `analytics.dim_user` | Review `status` and `user_id` | Convert `created_at` to UTC and normalize status values | Creation date, status totals, and distinct-user measures |
| `staging.hb_pitc` | `analytics.dim_health_board`, `analytics.dim_drug`, `analytics.dim_prescribed_type`, `analytics.fact_prescribing_monthly` | Inspect `HBT`, `DMDCode`, `BNFItemCode`, `BNFItemDescription`, and `PrescribedType` | Trim text, uppercase codes, and convert `PaidDateMonth` to a month-start `DATE` | Paid month, average cost per item, and average cost per quantity |
| `staging.device_classification` | `analytics.dim_device_classification` | Check `product_code`, `device_name`, and `definition` | Uppercase codes, convert Y/N indicators to Boolean values, and convert `last_updated` to a date | Device-class label and one current row per product under SCD Type 1 |

Technical filtering, such as selecting the required batch window and removing overlapping duplicates, will happen before business-specific exclusions are considered. Any business record excluded by a rule must be counted and documented. Records must never disappear from the workflow without an explanation in the run metadata or logs.

The scope table should be read as a processing map rather than a statement that every input field will be copied directly into every output. Some source columns are used for validation, some support joins, and others contribute to derived measures. The final output contains only the fields required by its data contract, but the transformation process should preserve the information needed to investigate any excluded or rejected row. This distinction helps keep analytics tables focused while maintaining accountability for source data.

The dependency order ensures that fact records can refer to stable dimensions and that common date or user information is available before summary calculations are completed. The workflow should not attempt to build a fact table while a required dimension is missing or only partially refreshed. A failed prerequisite should result in a controlled stop, a clear error message, and a recorded status for the affected batch. It is safer to delay handover than to publish facts with unresolved or invalid references.

The assumptions listed below describe the expected operating environment, but they should be checked whenever possible. For example, the workflow can verify whether the Stage 1 handover is approved, whether the expected columns exist, and whether timestamp values can be parsed. The assumption that a reference dataset is not available should also be revisited if an authoritative lookup is later provided. When an assumption changes, the team should update the specification, tests, and relevant output contracts together so that implementation and documentation remain aligned.

### 1.4 Dependencies and Assumptions

The planned processing sequence is as follows:

1. Prepare `dim_date`.
2. Build or refresh `dim_user`.
3. Build `dim_health_board`, `dim_drug`, `dim_prescribed_type`, and `dim_device_classification`.
4. Create `fact_prescribing_monthly`.
5. Create `fact_event_daily`, followed by `fact_daily_summary`.

The workflow relies on the following assumptions. These dependencies define the conditions under which the plan is expected to operate. They should be reviewed whenever the source files, API behavior, schedule, or business definitions change. An assumption is not a substitute for a validation check: where a condition can be verified automatically, the pipeline should verify it and record the result. If an assumption no longer holds, the team should update the documented contract and tests before relying on the affected output.



- Stage 1 has already completed its V1–V7 checks and its output has a handover decision of `APPROVED` or `APPROVED WITH WARNING`.
- Stage 1 timestamps are expected to be UTC already. Stage 2 will nevertheless enforce UTC and ISO 8601 formatting at its output-contract boundary.
- The three-day look-back can cause the same `log_id` or `entity_id` to occur in more than one business-date partition. Deduplication must therefore take place before daily facts are calculated.
- PITC records with a missing `DMDCode` or `BNFItemCode` may represent valid aggregate-level information. Those rows must be retained rather than treated automatically as invalid.
- The number of device-classification records returned by the API depends on the configured `API_SEARCH` setting. The current `.env` search, `product_code:NOB`, returns one row, while a search without a filter may return up to the configured API limit.
- No separate lookup datasets have been supplied for health-board names or drug names beyond the descriptions provided by the source. Until an official reference table is made available, `health_board_name` will use the health-board code. Unknown or aggregate records will be linked to designated unknown or aggregate dimension members instead of causing the batch to fail.
- Late-arriving records will be handled by reading the existing trailing staging window again. Historical extraction files will not be edited as part of transformation.

---

Profiling is the baseline assessment performed before the transformation rules alter the data. It provides evidence about what was received, which fields appear reliable, and where special handling may be required. The profile should be saved with the run metadata so the team can compare the current batch with earlier batches and explain unusual changes in volume or value distribution. A profile is not merely a report for documentation; it is an operational checkpoint that informs whether the batch is ready for the next processing step.

The profiling results should distinguish between observations and confirmed errors. For example, a high number of null values may be expected for an optional field, while the same pattern in a required key may indicate a serious quality problem. Likewise, a duplicate natural key may be caused by the intentional look-back window rather than by a defective source. The team should interpret profile findings in the context of the dataset's key definitions, business meaning, and documented rules before deciding whether to retain, deduplicate, quarantine, or escalate records.

## 2. Data Profiling and Identification of Data Quality Issues

Before transformation rules are applied, the staged data will be examined with Polars to understand its volume, key uniqueness, value distributions, and possible quality concerns. The profiling snapshot in this plan covers `raw_logs` and `raw_entities` for `business_date=2026-10-06`, `hb_pitc` for `business_date=2026-07-01`, and `device_classification` for `business_date=2026-10-27`.

For each input, the profile should record the number of rows read, the number of distinct natural keys, the number of null or blank values in important columns, and the observed range of relevant dates. Where applicable, it should also record duplicate counts, distinct status or event-type values, and the distribution of important numeric fields. These measurements help establish whether the batch is structurally similar to prior batches and whether any unexpected pattern needs review.

The snapshot dates and figures described in this plan should be treated as a documented example or baseline for the specified run, not as values to copy into future audit records. Every execution must calculate its own profile from the actual files it reads. If a count changes significantly, the run metadata should preserve the new value and any investigation notes. This allows reviewers to distinguish normal business variation from possible truncation, duplication, schema drift, or delayed source delivery.

### 2.1 Profile Summary

The profile summary is a baseline for understanding the batch before any changes are made. Row totals and distinct-key totals help show whether the input has the expected shape, while the field observations help identify values that require special handling. The figures should be retained with the run's metadata so that later batches can be compared with earlier ones. A change in volume does not necessarily indicate an error, but a sudden or unexplained change should be reviewed before the data is accepted.

| Dataset | Number of rows | Distinct natural keys | Profile observations |
|---|---:|---:|---|
| `raw_logs` | 525 | 525 `log_id` values | Five event types were observed. Timestamps ranged from 2026-10-05 02:33 UTC through 2026-10-06 23:59 UTC. |
| `raw_entities` | 200 | 200 `entity_id` values | Status distribution: PENDING 60, COMPLETED 54, CANCELLED 46, and ACTIVE 40. |
| `hb_pitc` | 122,141 | 122,141 composite keys | Sixteen distinct HBT values were present, all associated with paid month 2026-07. |
| `device_classification` | 1 | 1 `product_code` value | The current `product_code:NOB` search returned one record; the number can change with API settings. |

These figures describe the observed profile snapshot, not a guarantee that future batches will have the same counts. The pipeline must repeat profiling and validation for each run.

Each identified issue should be connected to a defined response. A value that can be corrected without changing its meaning may be cleaned automatically; a value that is valid but uncommon may be retained and monitored; and a value that cannot safely be interpreted may be quarantined for review. The chosen response should be consistent with the severity of the issue and the applicable rule. The same condition should not be silently corrected in one batch and rejected in another unless the documented policy or source context has changed.

Issue reporting should include the affected dataset, field, rule identifier, number of affected records, and the action taken. Where a rule has a threshold, the report should show both the observed value and the permitted limit. This supports transparent decisions and makes it possible to identify recurring problems that may require attention in Stage 1 or at the source. The transformation stage should not hide upstream quality problems simply because it can produce a technically valid output after applying a fallback.

### 2.2 Issues Detected and Planned Responses

The issue register connects each data-quality concern to a specific response. This is important because different issues require different treatments: a duplicate may be removed after a deterministic comparison, a valid aggregate record may be retained, and a malformed record that cannot be interpreted safely may need to be quarantined. The issue identifier should be referenced in tests and logs wherever applicable so the implementation, documentation, and audit report use the same terminology.

The profile identifies several issues that the transformation logic must be prepared to handle. Some issues were not present in the sampled partition but remain important risks because of how extraction works.

| Issue ID | Dataset / field | Potential problem | Observed example or count | Planned response |
|---|---|---|---|---|
| DQ-01 | `raw_logs.log_id` | A log may be repeated in overlapping extraction partitions | No duplicate in the single partition inspected; risk exists across multiple dates | Deduplicate by `log_id`, keeping the row with the newest `extracted_at` under C-01. |
| DQ-02 | `raw_logs.timestamp` | An event may arrive in a later extraction partition than its event date | An event at 2026-10-05 02:33 was found in the 2026-10-06 look-back window; 525 rows were read | Derive `event_date` from the event timestamp and recalculate affected dates using B-08. |
| DQ-03 | `raw_logs.payload` | Payload content may be malformed or empty instead of valid JSON | No malformed value was found among the 525 sampled rows | Replace invalid or empty payloads with `{}` and record the modified count under C-05. |
| DQ-04 | `raw_logs.event_type` | Capitalization or whitespace may differ between records | No current sample issue; five valid event values remained after normalization | Trim and lowercase values according to C-04 and S-02. |
| DQ-05 | `raw_logs.user_id` | A user identifier may be null or blank | No affected record in the current sample | Use `UNKNOWN_USER`, set `user_is_unknown=true`, and record the modification under C-03. |
| DQ-06 | `raw_entities.entity_id` | An entity may repeat across overlapping partitions | No duplicate in the single partition inspected | Deduplicate by entity key and keep the latest extraction under C-01. |
| DQ-07 | `raw_entities.status` | Status spelling or casing may be inconsistent | No issue in the current sample; four status values were observed | Trim and uppercase the value, then compare it with the controlled status set under C-04 and S-02. |
| DQ-08 | `hb_pitc.DMDCode` | Null values can be expected on aggregate prescribing rows | 38 records, or 0.03% | Keep these records and link them to the aggregate drug member under C-06. |
| DQ-09 | `hb_pitc.BNFItemCode` and `BNFItemDescription` | Aggregate rows may not have an item code or description | 1,597 records, or 1.31% | Retain the rows and use the aggregate drug member rather than rejecting them under C-06. |
| DQ-10 | `hb_pitc.PaidQuantity` | Quantity may be zero even when the paid-item count is positive | 13 records, or 0.01%; example: 40 paid items and quantity 0.0 | Keep the row and set `zero_quantity_flag=true` under C-08. |
| DQ-11 | `hb_pitc.HBT` | Health-board codes may not follow the usual format | 111 records, or 0.09%; examples include `SB0801` and `SB0806` rather than `S08xxxxxx` | Normalize and validate the code, map unrecognized values to `UNKNOWN`, and raise a warning under C-09 and S-08. |
| DQ-12 | PITC measures | Negative quantity or cost values may appear | No negative values in the current sample | If a negative or non-numeric measure is found, quarantine the affected row under C-07. |
| DQ-13 | `device_classification` snapshot | The same product may occur in multiple snapshots | No duplicate in the current one-row sample | Retain the latest version based on `extracted_at` or `last_updated` under C-10. |
| DQ-14 | Optional device attributes | Missing descriptive content may be represented as empty text | No empty `definition` in the current sample | Preserve empty text and convert Y/N indicators to Boolean values under S-06 and S-07 as applicable. |
| DQ-15 | All input schemas | A source column could be renamed, removed, or changed to an incompatible type | No unexpected schema change in the current profile | Run the contract checks before transformation and stop the batch if a required schema condition fails. |

The profile separates actual observations from possible problems. A zero count in the current sample does not remove the need for a rule, because a later extraction may contain the issue.

---

Cleaning rules define how the workflow responds to incomplete, inconsistent, or invalid records before analytical outputs are produced. The purpose is not to make every record look complete at any cost. Instead, the rules should preserve valid information, correct only what can be corrected reliably, and isolate records whose meaning cannot be determined with sufficient confidence. Each cleaning operation must be repeatable and measurable so its impact can be reviewed after the batch completes.

The implementation should maintain a clear distinction between a missing value, an empty string, a malformed value, and a value that is explicitly unknown. These states may require different treatment. For example, trimming whitespace can safely convert a string containing only spaces into a consistent empty representation, but it does not prove that a required identifier is valid. Similarly, mapping an absent reference code to an approved unknown dimension member preserves a fact's ability to participate in reporting, but it should not be interpreted as recovering the missing original code.

Cleaning should occur in a deliberate order. First, read and validate the expected schema. Next, normalize basic representations such as whitespace and case where the rule permits it. Then evaluate required fields, natural-key validity, duplicates, and business-specific conditions. Finally, separate accepted records from quarantined records and record counts for each outcome. This sequence helps ensure that validation uses consistent values and that a row rejected by one rule is not accidentally counted as accepted by a later step.

Every cleaning rule should have a stable identifier, a plain-language purpose, a defined condition, and an explicit action. Automated tests should cover normal examples as well as boundary cases, including nulls, blank strings, duplicate keys, malformed timestamps, and unexpected code values. If a rule changes, the team should update the documentation and tests at the same time and record the rule version used by the transformation run.

## 3. Data Cleaning Rules

The rules in this section establish a consistent way to handle records that do not perfectly match the expected input format. They should be applied in a predictable order, with key validation and deduplication completed before calculations that depend on row counts. The exact implementation should be covered by automated tests, including normal records, boundary cases, missing values, repeated records, and invalid values.

Cleaning is intended to improve consistency while preserving valid information. Records that must be rejected will be written to quarantine with enough metadata to explain why they were not accepted. At minimum, each rejected row will include `run_id`, `batch_id`, `rule_id`, and `rejection_reason`. Rows that are corrected and then retained will be counted as modified records and described in the run's `rules_applied` information.

| Rule | Name | Applies to | Condition | Processing action | Reason for the rule |
|---|---|---|---|---|---|
| C-01 | Deduplication across overlapping windows | `raw_logs.log_id`, `raw_entities.entity_id`, the PITC composite key, and `device_classification.product_code` | The same natural key occurs more than once across partitions or snapshots | Keep the record with the greatest `extracted_at`. If timestamps tie, use the greatest `run_id` as the tie-breaker. Track removed rows in `records_deduplicated`. | The extraction look-back intentionally reads recent data again. Keeping one latest record prevents repeated information from inflating totals. |
| C-02 | Reject missing primary-key values | All input datasets | A required primary-key component is null even though the extraction contract requires it | Send the row to quarantine and identify the rule as `C-02`. | Without a usable key, a row cannot be safely identified, joined, or deduplicated. |
| C-03 | Handle missing user identifiers | `raw_logs.user_id` and `raw_entities.user_id` | The user value is null, empty, or contains only whitespace | Replace it with `UNKNOWN_USER`, set `user_is_unknown=true`, and count the row as modified. | This preserves event and entity totals without making up a real person's identifier. |
| C-04 | Normalize whitespace | All string fields | A value has spaces at the beginning or end, or description text contains repeated internal spaces | Trim surrounding whitespace. For description fields, reduce repeated spaces to one. | Consistent text avoids mismatches in joins, comparisons, and dimension creation. |
| C-05 | Standardize payload JSON | `raw_logs.payload` | The payload is blank or cannot be parsed as valid JSON | Substitute `{}` and increment the modification count. Keep the original value in a separate audit copy only if the applicable policy permits it. | A malformed payload should not remove an otherwise valid event when the analytical facts do not depend on the payload content. |
| C-06 | Preserve aggregate prescribing rows | `hb_pitc.DMDCode`, `BNFItemCode`, and `BNFItemDescription` | Codes or descriptions are null because the row represents an aggregate level | Retain the record, assign the `AGGREGATE` member's `drug_key`, and set `is_aggregate=true`. | Aggregate prescribing information is analytically useful, so null drug codes in these rows are not automatically errors. |
| C-07 | Reject invalid numerical measures | `hb_pitc.NumberOfPaidItems`, `PaidQuantity`, and `GrossIngredientCost` | A measure is negative or cannot be converted to the expected numeric type | Quarantine the row and record the rule and affected column. | Negative prescribing volumes or costs are not valid under the stated business rules. |
| C-08 | Flag zero quantity | `hb_pitc.PaidQuantity` when `NumberOfPaidItems > 0` | Quantity equals zero even though the number of paid items is positive | Keep the record, set `zero_quantity_flag=true`, and count it as modified. | Some zero-quantity or zero-cost records may be legitimate. Rejecting them could understate paid-item totals. |
| C-09 | Map unrecognized health-board codes | `hb_pitc.HBT` | The code is blank or does not appear in the accepted reference list | Trim and uppercase the code, assign the `UNKNOWN` health-board member when it cannot be matched, retain the row, and issue a warning. | Keeping the row protects overall totals while making unfamiliar codes visible for later review. |
| C-10 | Select the current device snapshot | `device_classification.product_code` | More than one API snapshot contains the same product code | Keep the newest row according to `extracted_at` and/or `last_updated`. | A reference dimension should contain only the latest current version of each product. |
| C-11 | Protect facts from orphaned dimension references | Joins between facts and dimensions | A fact cannot find a matching dimension member | Assign the appropriate `UNKNOWN` or `AGGREGATE` key, count the correction, and retain the fact. | This maintains referential integrity without discarding a fact that can still contribute to reporting. |
| C-12 | Resolve changing entity status | `raw_entities.entity_id` | The same entity appears with different statuses in different time partitions | Use the newest `extracted_at` value for the current entity snapshot. For daily reporting, calculate counts using the deduplicated view and the appropriate creation date. | The latest extraction is authoritative for the current status, while date-based reporting must still use the correct daily grain. |

The cleaning process must maintain a distinction between a rejected record and a corrected record. A row that has been standardized or assigned an unknown dimension member can remain valid for downstream analysis; its modification should be visible in the audit metrics.

---

Standardization makes the same kind of information comparable across records and processing dates. Without consistent formats, equivalent values may be treated as different values during grouping, joining, filtering, or reporting. For that reason, date and timestamp parsing, text normalization, code casing, Boolean conversion, and numeric type assignment should be applied consistently and documented as part of the output contract.

Date and time handling deserves particular attention because a timestamp can represent the same instant while appearing under different local clock times. The plan requires UTC and ISO 8601 formatting at the output-contract boundary. The implementation should parse the source value using the expected format, handle timezone information explicitly, and avoid interpreting a timezone-naive value as local time unless that behavior is defined by the source contract. Date boundaries should be tested so that events near midnight are assigned to the intended business date.

Text normalization should be conservative. Trimming unnecessary surrounding whitespace and applying the documented casing rule to codes can improve consistency, but descriptive labels should not be rewritten in a way that changes their meaning. Boolean conversions should recognize only the values explicitly permitted by the rule; unexpected values should be reported rather than automatically treated as false. Numeric conversions should also define how invalid strings, decimal precision, and null values are handled.

The output types should be enforced before files are written. A field that happens to contain only numeric-looking strings in one batch must not be allowed to switch between string and integer types across different batches. Stable types allow downstream loading and analytics tools to interpret the data predictably. Any incompatible source change should be handled through a reviewed contract update rather than an untracked coercion.

## 4. Standardization and Data-Type Rules

The type and representation rules form a shared convention for all downstream tables. A standardized field should mean the same thing regardless of which input partition it came from. Conversion failures must remain visible in the quality results; silently coercing an invalid value to an arbitrary default could make later reports look complete while hiding a data problem.

Standardization ensures that equivalent values are represented consistently before joins, aggregations, and output writing. Identifiers must remain identifiers rather than being treated as ordinary numbers, and date/time values must use consistent formats.

| Rule | Field or fields | Possible input representation | Required representation | Handling if conversion or validation fails |
|---|---|---|---|---|
| S-01 | `timestamp`, `created_at`, `extracted_at`, `PaidDateMonth`, `last_updated` | ISO strings, date strings, `YYYYMM`, or timestamps with or without timezone information | UTC `TIMESTAMP` values using ISO 8601 at external boundaries; `PaidDateMonth` becomes a month-start `DATE` | Reject or quarantine the affected row using `S-01`. |
| S-02 | `event_type`, `status`, `PrescribedType` | Mixed capitalization or extra surrounding spaces | Lowercase `event_type`; uppercase `status` and `PrescribedType` | Quarantine values that do not belong to the controlled value set. |
| S-03 | `HBT`, `DMDCode`, `BNFItemCode`, `product_code`, `device_class` | Mixed-case codes or strings containing digits | Trimmed, uppercase text while preserving code formatting | Quarantine a value when it violates the required format under `S-03`. |
| S-04 | `BNFItemDescription`, `device_name`, `definition` | Free-form text with inconsistent spaces | Trimmed text with repeated whitespace collapsed; preserve original capitalization and punctuation | Retain the text; this rule does not require rejection when the text remains usable. |
| S-05 | `NumberOfPaidItems` | Integer values or numeric strings | A safe 32-bit `INTEGER` | Reject using `S-05` if conversion fails or the value is outside the supported range. |
| S-06 | `PaidQuantity`, `GrossIngredientCost` | Floating-point values or numeric strings | `DECIMAL(18,4)` for quantity and `DECIMAL(18,2)` for GBP cost | Reject using `S-06` if a valid decimal value cannot be produced. |
| S-07 | `implant_flag`, `life_sustain_support_flag`, `gmp_exempt_flag`, `third_party_flag` | Text indicators `Y` and `N` | Boolean values, where `Y=true` and `N=false` | For optional indicators with an unknown value, set `NULL` and raise a warning. |
| S-08 | `HBT` | Codes such as `S08xxxxxx` or non-standard values such as `SBxxxx` | Trimmed, uppercase code checked against the accepted health-board formats | Use the `UNKNOWN` member and record a warning when the code is not recognized. |
| S-09 | `device_class` | Text values `1`, `2`, `3`, `U`, or `N` | Controlled labels `CLASS_1`, `CLASS_2`, `CLASS_3`, `UNCLASSIFIED`, or `NOT_CLASSIFIED` | Keep the raw value and flag any classification that is not recognized. |
| S-10 | `user_id`, `entity_id`, `log_id` | UUID-like strings or other text identifiers | Trimmed text, retaining the identifier itself without numeric conversion | Quarantine missing required keys; use the unknown-user handling only for user references where permitted. |

Rounding must take place at the output boundary rather than prematurely during calculations. `PaidQuantity` will be rounded using half-up rounding to four decimal places, and `GrossIngredientCost` will be rounded to two decimal places. Average costs will be calculated from the unrounded staged values first and then rounded to four decimal places. This approach limits the effect of rounding on aggregate results.

---

Business transformation converts the cleaned and standardized source records into the analytical structures required by the project. The rules in this section should be treated as the authoritative definitions for how source information becomes dimensions, event-level facts, and summarized facts. This is important because a metric may be calculated in more than one report; if each report independently defines its own calculation, the results can differ even when they use the same source data.

Dimension processing should create stable descriptive members for the entities used in analysis. Fact processing should retain the appropriate grain, meaning the level of detail represented by one row. For example, an event-level fact and a daily summary fact serve different purposes and should not be treated as interchangeable. The grain of each output should be clear in its schema and key definitions so that users do not accidentally double-count measures when joining tables.

Aggregations should be calculated only after the relevant filtering and deduplication steps are complete. Otherwise, repeated records from overlapping partitions or one-to-many joins may inflate counts and costs. The process should document the grouping columns, measures, null-handling behavior, and rounding rules used for each metric. Validation should then compare the derived totals with suitable source-level counts or independent calculations wherever such comparisons are defined in the plan.

When a source record cannot be associated with a known dimension member, the workflow should follow the designated unknown-member policy rather than dropping the fact without explanation. This approach helps retain measurable activity while making the uncertainty visible to downstream users. Unknown-member rates should be monitored because a rising rate may indicate new source codes, a missing lookup, or a change in upstream data quality.

## 5. Business Transformation Rules

The business rules describe how the prepared data is organized for analysis. Dimensions provide descriptive context for grouping and filtering, while fact tables store measurable activity at a defined level of detail. The grain of each fact table must remain stable: daily event facts are grouped by their declared daily dimensions, while prescribing facts are grouped by the monthly prescribing keys. Changing the grain would change the meaning of the totals and therefore requires a reviewed contract change.

Business transformation converts cleaned fields into useful reporting dimensions, facts, and measures. The following rules define the intended calculations and the business purpose of each output.

| Rule | Source fields | Calculation or transformation | Output | Business purpose |
|---|---|---|---|---|
| B-01 | `timestamp`, `created_at`, `PaidDateMonth`, `last_updated` | Derive calendar dates and month-start periods, and prepare the date dimension | `event_date`, `created_date`, `paid_month`, `date_key`, `year`, `quarter`, `month`, `week`, `is_weekend` | Supplies a consistent date structure for time-based reporting. |
| B-02 | User identifiers and event/entity timestamps | Calculate each user's earliest and latest observed activity using minimum and maximum timestamps | `analytics.dim_user` with `first_seen_at` and `last_seen_at` | Supports distinct-user reporting and provides traceable user activity history. |
| B-03 | `raw_entities.status` | Count entities by creation date and normalized status | `entities_created`, `entities_active`, `entities_completed`, `entities_cancelled` in `fact_daily_summary` | Gives operational reports a consistent view of entity status totals. |
| B-04 | `raw_logs.event_type`, `log_id` | Count distinct log identifiers for each event date and event type | `analytics.fact_event_daily` with `event_count` | Allows event activity to be analyzed by date and category without hard-coding the event types into the fact structure. |
| B-05 | `fact_event_daily` and `dim_user` | Summarize event totals and distinct users by event date, and map recognized event types to named summary measures | `analytics.fact_daily_summary` | Creates a compact daily reporting dataset for common business measures. |
| B-06 | `hb_pitc.HBT` | Create one dimension member per accepted health-board code, use the code as the name until a reference table exists, and add an `UNKNOWN` member | `analytics.dim_health_board` | Enables health-board comparisons and keeps unmatched records joinable. |
| B-07 | `DMDCode`, `BNFItemCode`, `BNFItemDescription` | Create a member for each unique combination of drug identifiers and description; add one `AGGREGATE` member for null-code records | `analytics.dim_drug` | Supports drug-level analysis without losing aggregate prescribing rows. |
| B-08 | `hb_pitc.PrescribedType` | Build one dimension row for every valid controlled prescribed type | `analytics.dim_prescribed_type` | Ensures consistent reporting across AMP, VMP, AMPP, and VMPP types. |
| B-09 | PITC measure columns | Aggregate at `(health_board_key, drug_key, prescribed_type_key, paid_month)`; calculate paid items, paid quantity, gross ingredient cost, average cost per item, and average cost per quantity | `analytics.fact_prescribing_monthly` | Provides monthly prescribing volume and cost measures. |
| B-10 | Device-classification attributes | Retain one current record per product code, replace changed attributes under SCD Type 1, convert Y/N flags to Boolean values, and map classification labels | `analytics.dim_device_classification` | Maintains a current and consistent device reference dimension. |
| B-11 | `PaidQuantity`, `NumberOfPaidItems` | Set `zero_quantity_flag` when quantity is zero and paid items are greater than zero | `fact_prescribing_monthly.zero_quantity_flag` | Makes the 13 zero-quantity records in the profiled month visible to analysts. |
| B-12 | Fact records | Do not exclude log or entity records from `fact_daily_summary` simply because their status is pending or cancelled; if a future business exclusion is added, record its count and reason | `records_excluded` metric | Prevents meaningful operational statuses from being silently removed. |
| B-13 | Natural keys | Generate stable integer surrogate keys by hashing the natural key with a stable namespace | Columns ending in `_key` | Keeps joins stable across reruns and prevents key values from depending on input row order. |

Joins should be planned around the expected relationship between the participating datasets. Before joining, the workflow should check that the lookup side has the required key uniqueness and that the join columns use compatible types and standardized representations. After joining, the workflow should compare row counts and verify that the result has the expected cardinality. These checks help detect situations where duplicate dimension keys multiply fact rows or where an unexpected null key causes records to become unmatched.

A left join is appropriate when the fact record must be retained even if a matching dimension value is not available, provided the documented unknown-member policy is applied. An inner join should be used only when excluding unmatched records is explicitly justified by the business rule. Join behavior should never be selected merely because it makes the output easier to produce. Any unmatched rows should be counted, categorized, and reported so reviewers can determine whether the outcome is acceptable.

### Join and Lookup Policy

Join checks are especially important because a technically valid join can still produce incorrect totals if a lookup key matches multiple dimension rows. The pipeline should compare row counts before and after joins where applicable and validate that each fact row resolves to the expected dimension member. Unknown and aggregate members are controlled ways to retain valid facts when detailed lookup information is unavailable; they should not be used to hide a broken lookup table or an unexpected schema change.

Fact tables must be linked to their dimensions using the keys defined by the output contracts. Lookup dimensions must first be deduplicated by their natural keys. Every dimension lookup is expected to be many-to-one from the fact table's perspective. If a lookup unexpectedly returns multiple dimension rows for one natural key, validation must fail because the join could multiply fact records and inflate measures.

| Fact-to-dimension relationship | Join key | Join approach | Handling for unmatched values |
|---|---|---|---|
| `fact_prescribing_monthly` to `dim_health_board` | `health_board_key` | Inner join after unknown-member assignment | Use the `UNKNOWN` board member and increase the warning count. |
| `fact_prescribing_monthly` to `dim_drug` | `drug_key` | Inner join after aggregate or unknown assignment | Use the `AGGREGATE` member for aggregate rows. |
| `fact_prescribing_monthly` to `dim_prescribed_type` | `prescribed_type_key` | Inner join | Quarantine a row if its type is outside the controlled set. |
| `fact_event_daily` to `dim_date` | `event_date_key` | Inner join | Quarantine records whose dates fall outside the valid date dimension. |
| `fact_event_daily` to `dim_user` | `user_key` | Inner join after assigning `UNKNOWN_USER` where needed | Link to the `UNKNOWN_USER` member. |
| `fact_daily_summary` to `dim_date` | `event_date_key` | Inner join | The design expects exactly one matching date row for each summary record. |

These join requirements protect the accuracy of the output totals. The transformation must not continue as though it succeeded if an unexpected one-to-many lookup creates additional rows.

---

The data contract is the formal description of what Stage 2 promises to deliver. It identifies the output datasets, field names, data types, key expectations, partition conventions, and quality requirements that the Load stage can rely on. A clear contract reduces ambiguity between the team producing the transformed data and the team consuming it. It also gives automated tests a stable target against which to check every output batch.

The contract should be maintained together with the transformation implementation. If a column is renamed, a data type changes, a key definition is updated, or a new output is introduced, the team should assess the effect on downstream loading and reporting before making the change. Documentation, schema checks, and tests should be updated as part of the same reviewed change. Unannounced schema changes can cause downstream failures or, more dangerously, allow data to load with an incorrect interpretation.

## 6. Transformed Data Contract and Naming Standards

The transformed outputs need a consistent structure so that the Load stage can process them without interpreting every dataset separately. Table names, column names, key fields, timestamps, and file paths will follow the conventions below.

Consistent naming helps users and automated jobs locate the correct output without relying on informal knowledge. Dataset names should communicate their analytical role, while column names should remain stable and meaningful. Partition paths should follow the documented business-date or month convention so that a downstream process can select the intended data range without scanning unrelated files.

The output directory should also make reruns and audit review straightforward. A manifest should identify the transformation run, its parent extraction run, the output datasets produced, the relevant partitions, the schema or contract version, and the validation status. The manifest should describe files that actually exist and have passed the required checks; it should not be generated as if a write succeeded when the output is incomplete. Temporary files should not be exposed as final outputs before the write and validation process is complete.

### 6.1 Naming and File Organization

Consistent names and directory patterns make the output easier to discover and automate. The business-date or month partition identifies the period represented by a file, while the manifest records which run created it and which checks were completed. A consumer should be able to locate a dataset using the documented convention without relying on a manually maintained list of filenames. File writes should be completed in a controlled manner so downstream processing does not read a partially written partition.

- **Schema name:** `analytics`.
- **Dimension tables:** use the `dim_` prefix.
- **Fact tables:** use the `fact_` prefix.
- **Column names:** use `snake_case`.
- **Natural identifiers:** use suffixes such as `_id` or `_code`, for example `user_id` and `health_board_code`.
- **Surrogate identifiers:** use the `_key` suffix, for example `user_key` and `drug_key`.
- **Timestamps:** use `_at`; calendar dates use `_date`; Boolean indicators use `is_` or `_flag`.
- **Transformation lineage:** store the transformation run identifier in `transformed_run_id`. Keep the source extraction identifier in transformation metadata as `parent_run_id`.
- **Daily output files:** `data/transformed/<table>/business_date=YYYY-MM-DD/data.parquet`.
- **Monthly fact and dimension partitions:** use a month-start partition such as `business_date=YYYY-MM-01/data.parquet`.
- **Handover manifest:** `data/transformed/_handover/business_date=YYYY-MM-DD/manifest.json`.

Each output schema defines the fields required for its intended analytical purpose. The implementation should validate both the presence and the type of every required field, and it should check key uniqueness where the contract requires a unique row per key or key combination. Optional fields may contain null values only when that behavior is allowed by the contract. Required fields should be evaluated using explicit rules rather than assumptions based on the values seen in a single sample batch.

The schemas should be read together with the source-to-target mapping and transformation rules. A schema states what the final output looks like, while the mapping explains where the values come from and how they are produced. Neither document alone is sufficient to explain the full transformation. Together, they allow a reviewer to trace a target column back to its source fields and verify that its business meaning has been preserved.

### 6.2 Required Output Schemas

The schema definitions that follow are the expected interface for the Load stage. Each dataset should be produced with the declared column names and compatible types, and the primary or natural key behavior should be checked before publication. The output should not contain accidental helper columns from intermediate processing unless those fields are explicitly part of the contract. Any schema change should be intentional and accompanied by updates to the relevant validation tests and handover documentation.

The following definitions describe the required output fields, their data types, nullability, and key relationships. The Load stage must use these contracts as the expected structure of the transformed data.

#### `analytics.dim_date`

| Column | Type | Null allowed? | Key or requirement |
|---|---|---|---|
| `date_key` | INTEGER | No | Primary key |
| `calendar_date` | DATE | No | Unique date |
| `year` | INTEGER | No | Calendar year |
| `quarter` | INTEGER | No | Calendar quarter |
| `month` | INTEGER | No | Month number |
| `day` | INTEGER | No | Day of month |
| `week_of_year` | INTEGER | No | Week number |
| `is_weekend` | BOOLEAN | No | Indicates whether the date is a weekend |

#### `analytics.dim_user`

| Column | Type | Null allowed? | Key or requirement |
|---|---|---|---|
| `user_key` | BIGINT | No | Primary key |
| `user_id` | STRING | No | Unique natural identifier |
| `first_seen_at` | TIMESTAMP (UTC) | No | Earliest observed timestamp |
| `last_seen_at` | TIMESTAMP (UTC) | No | Latest observed timestamp |
| `user_is_unknown` | BOOLEAN | No | Indicates the unknown-user member |
| `updated_at` | TIMESTAMP (UTC) | No | Last update time |

#### `analytics.dim_health_board`

| Column | Type | Null allowed? | Key or requirement |
|---|---|---|---|
| `health_board_key` | INTEGER | No | Primary key |
| `health_board_code` | STRING | No | Unique natural identifier |
| `health_board_name` | STRING | No | Uses the code until a name reference is available |
| `is_unknown` | BOOLEAN | No | Identifies the unknown member |

#### `analytics.dim_drug`

| Column | Type | Null allowed? | Key or requirement |
|---|---|---|---|
| `drug_key` | BIGINT | No | Primary key |
| `dmd_code` | BIGINT | Yes | Part of the natural key |
| `bnf_item_code` | STRING | Yes | Part of the natural key |
| `bnf_item_description` | STRING | Yes | Descriptive label |
| `is_aggregate` | BOOLEAN | No | Marks aggregate drug rows |
| `updated_at` | TIMESTAMP (UTC) | No | Last update time |

#### `analytics.dim_prescribed_type`

| Column | Type | Null allowed? | Key or requirement |
|---|---|---|---|
| `prescribed_type_key` | INTEGER | No | Primary key |
| `prescribed_type_code` | STRING | No | Unique natural identifier |
| `prescribed_type_description` | STRING | No | Description of the prescribed type |

#### `analytics.dim_device_classification`

| Column | Type | Null allowed? | Key or requirement |
|---|---|---|---|
| `device_key` | BIGINT | No | Primary key |
| `product_code` | STRING | No | Unique natural identifier |
| `device_name` | STRING | No | Device name |
| `device_class` | STRING | No | Normalized classification |
| `regulation_number` | STRING | Yes | Regulation identifier, when supplied |
| `medical_specialty_description` | STRING | Yes | Specialty description, when supplied |
| `review_panel` | STRING | Yes | Review panel, when supplied |
| `implant_flag` | BOOLEAN | Yes | Optional implant indicator |
| `life_sustain_support_flag` | BOOLEAN | Yes | Optional life-support indicator |
| `gmp_exempt_flag` | BOOLEAN | Yes | Optional GMP exemption indicator |
| `third_party_flag` | BOOLEAN | Yes | Optional third-party indicator |
| `submission_type_id` | STRING | Yes | Submission type identifier |
| `definition` | STRING | Yes | Device definition |
| `last_updated` | DATE | No | Source update date |
| `updated_at` | TIMESTAMP (UTC) | No | Transformation update time |

#### `analytics.fact_prescribing_monthly`

| Column | Type | Null allowed? | Key or requirement |
|---|---|---|---|
| `prescribing_key` | BIGINT | No | Primary key |
| `health_board_key` | INTEGER | No | Foreign key to `dim_health_board` |
| `drug_key` | BIGINT | No | Foreign key to `dim_drug` |
| `prescribed_type_key` | INTEGER | No | Foreign key to `dim_prescribed_type` |
| `period_key` | INTEGER | No | Foreign key to `dim_date` |
| `paid_month` | DATE | No | First day of the paid month |
| `paid_items` | INTEGER | No | Must be at least zero |
| `paid_quantity` | DECIMAL(18,4) | No | Must be at least zero |
| `gross_ingredient_cost` | DECIMAL(18,2) | No | Must be at least zero |
| `avg_cost_per_item` | DECIMAL(18,4) | Yes | Null when `paid_items = 0` |
| `avg_cost_per_quantity` | DECIMAL(18,4) | Yes | Null when `paid_quantity = 0` |
| `zero_quantity_flag` | BOOLEAN | No | Identifies zero quantity with positive item count |
| `transformed_run_id` | STRING | No | Transformation-run lineage |
| `updated_at` | TIMESTAMP (UTC) | No | Last update time |

The unique natural key for this table is `(health_board_key, drug_key, prescribed_type_key, period_key)`.

#### `analytics.fact_event_daily`

| Column | Type | Null allowed? | Key or requirement |
|---|---|---|---|
| `event_fact_key` | BIGINT | No | Primary key |
| `event_date_key` | INTEGER | No | Foreign key to `dim_date` |
| `user_key` | BIGINT | No | Foreign key to `dim_user` |
| `event_type` | STRING | No | Standardized event category |
| `event_count` | BIGINT | No | Must be at least zero |
| `transformed_run_id` | STRING | No | Transformation-run lineage |

The unique natural key for this table is `(event_date_key, user_key, event_type)`.

#### `analytics.fact_daily_summary`

| Column | Type | Null allowed? | Key or requirement |
|---|---|---|---|
| `summary_key` | BIGINT | No | Primary key |
| `event_date_key` | INTEGER | No | Unique foreign key to `dim_date` |
| `total_events` | BIGINT | No | Must be at least zero |
| `unique_users` | BIGINT | No | Must be at least zero |
| `signup_count` | BIGINT | No | Must be at least zero |
| `purchase_count` | BIGINT | No | Must be at least zero |
| `view_count` | BIGINT | No | Must be at least zero |
| `click_count` | BIGINT | No | Must be at least zero |
| `logout_count` | BIGINT | No | Must be at least zero |
| `entities_created` | BIGINT | No | Must be at least zero |
| `entities_active` | BIGINT | No | Must be at least zero |
| `entities_completed` | BIGINT | No | Must be at least zero |
| `entities_cancelled` | BIGINT | No | Must be at least zero |
| `transformed_run_id` | STRING | No | Transformation-run lineage |

The source-to-target mapping is the traceability link between the staging data and the analytical output. For each target field, it should identify the source field or fields, the transformation rule applied, the expected type, and any fallback or null-handling behavior. If a target field is derived from multiple source values, the mapping should explain the calculation at a level that can be reviewed and tested.

Mapping documentation is particularly useful when investigating discrepancies. If a total differs from the source expectation, the team can trace the target measure through its contributing fields, filtering conditions, join keys, and aggregation level. A field that has no direct source equivalent should be identified as derived and linked to its business rule. This avoids ambiguity about whether a value was copied, normalized, looked up, or calculated.

### 6.3 Source-to-Target Field Mapping

The source-to-target mapping provides traceability between input fields and the final analytics model. It clarifies whether a target value is copied, standardized, derived from one or more source columns, or assigned through a dimension lookup. This mapping is useful when investigating an unexpected report value because it points reviewers toward the source field and the rule responsible for the transformation. If a target field has no direct source column, its derivation should be documented rather than assumed.

This mapping explains how selected source fields will be converted into their corresponding output fields.

| Source dataset | Source field | Destination field | Source type | Destination type | Required action |
|---|---|---|---|---|---|
| `raw_logs` | `log_id` | `fact_event_daily.event_count` | STRING | BIGINT | Count distinct identifiers after deduplication with C-01. |
| `raw_logs` | `event_type` | `fact_event_daily.event_type` | STRING | STRING | Trim and lowercase according to C-04 and S-02. |
| `raw_logs` | `timestamp` | `fact_event_daily.event_date_key` | TIMESTAMP (UTC) | INTEGER | Derive the event date and look up its key in `dim_date` under B-01. |
| `raw_logs` | `user_id` | `fact_event_daily.user_key` | STRING | BIGINT | Apply unknown-user handling, then look up the corresponding `dim_user` key. |
| `raw_logs` | `log_id` | `fact_daily_summary.total_events` | STRING | BIGINT | Count distinct log identifiers across event types under B-05. |
| `raw_logs` | `user_id` | `fact_daily_summary.unique_users` | STRING | BIGINT | Count distinct users after unknown-user handling. |
| `raw_entities` | `created_at` | `fact_daily_summary.event_date_key` | TIMESTAMP (UTC) | INTEGER | Derive the creation date and map it to `dim_date`. |
| `raw_entities` | `status` | `entities_*` measures | STRING | BIGINT | Use conditional counts based on normalized statuses. |
| `hb_pitc` | `HBT` | `dim_health_board.health_board_code` | STRING | STRING | Uppercase and validate the code under C-09 and S-08. |
| `hb_pitc` | `DMDCode`, `BNFItemCode` | `dim_drug` natural key | INTEGER, STRING | BIGINT, STRING | Preserve identifiers and assign the aggregate member when the codes are null. |
| `hb_pitc` | `PaidDateMonth` | `fact_prescribing_monthly.paid_month` | TIMESTAMP (UTC) | DATE | Convert to the first day of the paid month under S-01. |
| `hb_pitc` | `NumberOfPaidItems` | `fact_prescribing_monthly.paid_items` | INTEGER | INTEGER | Convert and validate using S-05. |
| `hb_pitc` | `PaidQuantity` | `fact_prescribing_monthly.paid_quantity` | FLOAT | DECIMAL(18,4) | Apply half-up rounding using S-06. |
| `hb_pitc` | `GrossIngredientCost` | `fact_prescribing_monthly.gross_ingredient_cost` | FLOAT | DECIMAL(18,2) | Apply half-up rounding using S-06. |
| `hb_pitc` | `GrossIngredientCost`, `NumberOfPaidItems` | `avg_cost_per_item` | FLOAT, INTEGER | DECIMAL(18,4) | Derive the average cost per paid item under B-09. |
| `device_classification` | `product_code` | `dim_device_classification.product_code` | STRING | STRING | Normalize to uppercase and deduplicate under C-10 and S-03. |
| `device_classification` | Flag fields | Corresponding `*_flag` fields | STRING | BOOLEAN | Convert `Y` and `N` to Boolean values using S-07. |
| `device_classification` | `last_updated` | `dim_device_classification.last_updated` | TIMESTAMP (UTC) | DATE | Convert the source timestamp to a date using S-01. |

Schema compatibility checks should run before the output is handed to the Load stage. The checks should detect missing required columns, unexpected type changes, incompatible date formats, duplicate field names, and key fields that cannot be interpreted as expected. If an incompatibility could alter the meaning of the output, the batch should be stopped until the issue is reviewed. Automatic conversion is appropriate only where the conversion is explicitly allowed and tested.

Compatibility should be evaluated against the agreed contract version, not merely against the previous output file. Comparing only with the last batch can allow an earlier accidental change to become the new standard. The contract is the reference point for determining whether the output is valid. Any intentional contract revision should be versioned and communicated to the Load stage before the revised dataset is delivered.

### 6.4 Schema Compatibility

Compatibility checks should distinguish between changes that can be safely handled and changes that could alter the meaning of the data. A new optional source field may not require a change to the output contract, whereas removal of a required field or a type change affecting calculations should stop processing. The pipeline should report the name of the incompatible field and the expected versus observed condition so the team can resolve the problem efficiently.

Before transformation writes any output, the engine will compare the incoming columns and their types with the expected input contracts. A required field that is missing, renamed, or changed to an incompatible type will cause the batch to fail before the output is published. Newly added nullable fields will be ignored until the contract is deliberately updated.

Any approved contract change must be recorded with a new `transformation_version`. The Load stage must then be coordinated with that updated contract so that the consumer and producer continue to agree on field names, types, and meaning.

---

Validation is the evidence that the transformation produced a result suitable for downstream use. It should cover structural correctness, data quality, business logic, and reconciliation. Structural checks confirm that the output follows the expected schema. Data-quality checks evaluate nulls, duplicates, invalid values, and reference integrity. Business checks confirm that derived measures follow their documented definitions. Reconciliation checks compare the input and output counts in ways that account for aggregation, deduplication, and quarantine.

Each validation check should have an identifier, a clear description, an expected result or threshold, an observed result, and a status. A failed check should produce an actionable message that names the affected dataset or partition. Critical failures should prevent handover. Warnings may be permitted only where the plan allows them and the impact has been assessed. The workflow should not turn a failure into a warning simply to complete the scheduled run.

Validation results should be retained with the run's metadata. This creates a record of why the batch was accepted, accepted with a warning, or rejected. When a batch is rerun after a correction, the new results should be linked to the rerun rather than overwriting the history of the earlier failure. That record helps the team confirm that a correction addressed the original problem and did not introduce a different issue.

## 7. Transformation Validation and Data Quality Controls

Validation is the evidence that the transformation has produced a trustworthy output, rather than simply a collection of files. The checks should be repeatable and should produce measurable results, such as row counts, duplicate counts, null counts, unmatched-key counts, and comparison results. A validation result must be associated with the run and the output dataset it concerns. This lets the handover decision be reproduced later and helps identify whether a failure originated in the input, transformation logic, or output-writing process.

Validation will confirm that the transformation has preserved the expected records, generated unique keys, maintained valid relationships between facts and dimensions, and followed the declared data types and business rules. The checks are designed to run before a batch is approved for handover.

The row reconciliation calculation is:

```text
records_read = records_output
             + records_rejected
             + records_deduplicated
             + records_excluded
```

`records_modified` is tracked separately because a modified row is still retained in the output. Its changes are recorded in `rules_applied`, rather than counted as a separate category in the reconciliation equation.

| Check ID | Validation | Applies to | What it verifies | Passing condition | Severity |
|---|---|---|---|---|---|
| TV-01 | Row reconciliation | All transformed tables | Confirms that every input row is accounted for | The reconciliation equation balances exactly. | Critical |
| TV-02 | Primary-key uniqueness | All output tables | Prevents duplicate fact or dimension records | No duplicate declared primary keys are found. | Critical |
| TV-03 | Required-field completeness | Required columns in `dim_*` and `fact_*` | Ensures keys and required measures are populated | Every `NOT NULL` column has a null count of zero. | Critical |
| TV-04 | Referential integrity | Fact-table foreign keys | Checks that fact references resolve to dimension members | Every foreign key matches a dimension key, or the row has been assigned an allowed unknown member. | Critical |
| TV-05 | Join cardinality | Dimension lookups before fact generation | Detects duplicate lookup keys that could multiply rows | Natural keys are unique and joining does not increase the row count beyond the deduplicated input. | Critical |
| TV-06 | Data type and format conformance | All contracted fields | Confirms compatibility with the output contract | Values use the required types, UTC timestamps, ISO date representation, and decimal precision. | Critical |
| TV-07 | Business-value validation | PITC and daily measures | Finds invalid negative measures | No negative paid-item, quantity, cost, event-count, or unique-user measures are present. | Critical |
| TV-08 | PITC aggregate reconciliation | `fact_prescribing_monthly` | Confirms that transformation has not lost accepted prescribing measures | Sums of paid items, paid quantity, and gross ingredient cost agree with accepted input totals within 0.01 tolerance for costs and quantities. | Critical |
| TV-09 | Event-count reconciliation | `fact_event_daily` and `fact_daily_summary` | Checks that daily event totals match deduplicated logs | Event totals agree with deduplicated `log_id` counts for each date, and the summary total equals the sum of event rows. | Critical |
| TV-10 | Quarantine completeness | Quarantine storage | Confirms that all rejected rows are documented | Rejection counts match quarantine row counts, and each row has a `rule_id` and `rejection_reason`. | Critical |
| TV-11 | Current-dimension integrity | `dim_device_classification` and `dim_drug` | Prevents duplicate current members for a natural key | There is exactly one current member per natural key. | Critical |
| TV-12 | Zero-quantity reporting | `fact_prescribing_monthly.zero_quantity_flag` | Ensures zero-quantity cases are retained and visible | The number of flagged rows matches the accepted zero-quantity cases; the profiled month is expected to contain 13. | Non-critical |
| TV-13 | Unknown-member percentage | Unknown user, board, and drug members | Highlights unexpectedly high rates of unmatched values | Unknown-member share is no more than 1%; a higher share produces a warning. | Non-critical |
| TV-14 | Data freshness | All output partitions | Detects stale outputs | Maximum `updated_at` falls within the batch window and is not earlier than the corresponding extraction's `extracted_at`. | Non-critical |

Checks that belong specifically to extraction will not be repeated as transformation checks. Instead, Stage 2 will refer to the extraction audit metadata when it needs to verify lineage or reconcile the input handover. Source connectivity, extraction completeness, and the Stage 1 V1–V7 results remain responsibilities of Stage 1.

---

Transformation metadata describes the execution itself, while operational logs provide the details needed to understand what happened during that execution. Together, they support monitoring, troubleshooting, audit review, lineage, and recovery. The metadata should identify the transformation run and its parent extraction run, the selected batch window, the code or rule version, start and end times, execution status, output locations, and validation summary. Where a task fails, the record should contain a useful error category and a concise message.

Logging should be informative without exposing unnecessary source content. Full payloads and sensitive values should not be copied into routine logs when an identifier and a clear error reason are sufficient. Log messages should distinguish expected data-quality outcomes from technical failures, such as unavailable storage or a parsing exception. Consistent event names and structured fields make the logs easier to search and allow Airflow or other monitoring tools to identify recurring patterns.

Metadata must reflect the actual outcome of the run. A status should not be marked successful until the required output writes and validation steps have completed. If processing stops after producing some temporary files, those files should not be represented as a fully accepted batch. The recorded status, manifest, and output directory should agree so that the next stage cannot mistake an incomplete result for a ready-to-load dataset.

## 8. Transformation Metadata and Logging

Logging and metadata are intended to support troubleshooting without requiring the team to rerun the entire process to understand what happened. A run should have a unique identifier, reference its parent extraction run, and record its batch window and transformation version. Counts should be captured at meaningful stages, including records read, records retained, records modified, records deduplicated, records quarantined, and records written. When a step fails, the logs should identify the failed operation and relevant rule or validation check.

Each transformation run must produce audit metadata and an operational log. These records make it possible to identify what was processed, which extraction produced the input, which rules changed data, whether validation succeeded, and why a batch may have failed.

The transformation audit database will be located at `data/metadata/sightsync_transform_audit.db`, using the `transformation_runs` table. In addition, each run will create a JSON log at `data/logs/transformation/<business_date>/<run_id>.json`.

| Metadata field | Meaning | Example |
|---|---|---|
| `run_id` | Unique identifier for a transformation execution | `TRF_20261027_0200_a1b2c3` |
| `parent_run_id` | Identifier of the extraction run that supplied the input | `a8f3b1e2-9c10-4e88-b223-112233445566` |
| `batch_id` | Stable identifier for the processing window and idempotency behavior | `sightsync_transform:2026-10-27` |
| `batch_window_start` | Earliest business date included in the batch | `2026-10-24` |
| `batch_window_end` | Latest business date included in the batch | `2026-10-27` |
| `table_name` | Output table or file being produced | `analytics.fact_daily_summary` |
| `transformation_started_at` | UTC time when transformation began | `2026-10-27T02:05:00Z` |
| `transformation_ended_at` | UTC time when transformation finished | `2026-10-27T02:05:42Z` |
| `duration_ms` | Total elapsed execution time in milliseconds | `42000` |
| `status` | Final run state | `SUCCESS`, `FAILED`, or `QUARANTINED` |
| `records_read` | Number of rows accepted as input to the processing batch | `525` |
| `records_output` | Number of rows written to transformed output | `520` |
| `records_rejected` | Number of rows sent to quarantine | `2` |
| `records_deduplicated` | Number of overlapping rows removed | `3` |
| `records_excluded` | Number of records removed by approved business exclusions | `0` |
| `records_modified` | Number of retained records that were changed by cleaning or standardization | `525` |
| `rules_applied` | JSON object containing the number of rows affected by each rule | `{"C-01":3,"C-03":0,"S-02":525}` |
| `validation_results` | JSON summary of individual validation statuses | `{"TV-01":"PASS","TV-02":"PASS"}` |
| `error_code` | Stable code for identifying the failure type | `TV-04_REFERENTIAL_INTEGRITY` |
| `error_message` | Human-readable explanation of the failure | `fact_prescribing_monthly has 4 orphan drug_key values` |
| `transformation_version` | Version of the code and transformation contract | `2.0.0` |

The recorded metadata supports several operational needs:

- **Monitoring:** Airflow can inspect `status`, `duration_ms`, and `validation_results` to identify failed or unusually slow batches.
- **Troubleshooting:** `error_code`, `error_message`, and `parent_run_id` help locate the problem and the extraction run connected to it.
- **Auditing:** `rules_applied`, `records_modified`, and the validation history show what changed during processing and which checks were performed.
- **Lineage:** The combination of `parent_run_id` and `transformed_run_id` connects transformed outputs to the extraction batch that produced their source data.
- **Recovery:** `batch_id` and the recorded batch window allow a failed batch to be rerun without disturbing unrelated partitions.

The example counts above illustrate the metadata format. Actual values must be generated from the run itself rather than copied from the example.

---

Retention and access rules protect the integrity and confidentiality of the transformation process while preserving the evidence needed for investigation. Different information types may have different operational purposes and retention periods, as listed in the existing table. These periods should be applied consistently and reviewed against applicable institutional requirements. If an incident, audit, or unresolved defect requires records to be retained longer, the responsible owner should document that decision rather than allowing routine cleanup to remove relevant evidence.

Quarantine should be treated as a controlled workflow state. A quarantined record remains available for investigation, but it must not be counted as accepted analytical output. Its rejection reason should identify the rule that failed and provide enough context for an authorized reviewer to decide whether the source needs correction, the rule needs clarification, or the record should remain excluded. Reprocessing should be performed through a documented rerun so that the accepted output and audit trail remain consistent.

Access should follow the principle of least privilege. Service accounts should receive only the permissions required to read staging data, write transformation outputs, and record metadata. Human access should be limited to staff who need the information for their responsibilities. Write and delete permissions deserve particular control because an unauthorized change could alter audit evidence or remove rejected records before they are reviewed. Permission changes should be recorded and periodically checked.

The retention policy should also consider backup and cleanup behavior. Removing a file from the main directory does not necessarily remove copies in backups or cold storage. The team should understand how the documented retention periods apply across those locations and ensure that expired information is handled consistently. These controls support reliable operations without expanding the scope of Stage 2 beyond transformation, audit, and controlled handover.

## 9. Log Retention, Quarantined Records, and Access Control

Quarantine is a controlled holding area for rows that cannot safely be included in the accepted output. It is not a place to discard inconvenient records or to bypass investigation. The stored reason should be specific enough for an authorized reviewer to understand the failure and determine whether the source data, transformation rule, or contract needs attention. If a corrected batch is rerun, the new run should preserve a link to the original batch and its quarantine details so the history remains understandable.

Operational logs, transformation metadata, rejected records, and validation results must be retained long enough to support troubleshooting and audit requests. Access should be restricted according to each user's responsibilities, particularly when records contain identifiers or fragments of payload data.

| Information category | Storage location | Retention period | Purpose |
|---|---|---|---|
| Transformation operational logs | `data/logs/transformation/<business_date>/` | 90 days | Supports routine investigation, monitoring, and batch recovery. |
| Transformation audit metadata | `data/metadata/sightsync_transform_audit.db` | 24 months | Provides a historical record for verification and audit work, consistent with Stage 1 retention. |
| Rejected records in quarantine | `data/quarantine/<table>/business_date=YYYY-MM-DD/rejected.parquet` | 24 months | Keeps rejected data available for correction, reprocessing, audit, and dispute resolution. |
| Validation results | Audit database and per-run JSON log | 24 months with the audit records | Demonstrates that handover decisions were based on documented checks. |

Every quarantined record should retain the source fields needed to investigate or repair it. The quarantine dataset must also include:

- `run_id`, `batch_id`, and `parent_run_id`;
- `rule_id`, `rejection_reason`, and `rejected_at`;
- `source_table` and `business_date`;
- the source columns required to understand and reprocess the rejected row.

Access permissions will be applied as follows:

- **Write access:** limited to the Airflow transformation service account and authorized Data Engineering personnel.
- **Read access:** available to Data Engineering, the Data Lead, and internal audit when required for their duties.
- **Delete access:** restricted to the scheduled retention process and subject to Data Lead approval.
- **Quarantine access:** limited to Data Engineering and the Data Lead because rejected records may include user identifiers or payload fragments.

Logs and quarantined records must not be deleted before their retention period expires or while they are needed for an unresolved incident. Operational logs should avoid storing full payloads or unnecessary sensitive source values. Identifiers and concise rejection reasons should be used whenever possible. After the first 90 days, operational JSON logs may be moved to compressed cold storage for the remainder of the 24-month audit period.

---

Acceptance criteria define the conditions that must be met before the transformed datasets can be passed to the Load stage. The criteria should be objective and verifiable, rather than based only on whether the job completed. They include successful processing of the approved input partitions, conformity to the output contracts, completion of required validation checks, reconciliation of record counts, correct handling of rejected records, and availability of the manifest and audit information.

The handover package should make it possible for the receiving stage to identify exactly what is being delivered and what checks have been completed. It should include the output locations, business-date or monthly partitions, transformation run identifier, parent extraction run identifier, schema or contract version, validation summary, and any approved warnings. If any required item is missing, the handover should be considered incomplete until the missing evidence is supplied.

The Load stage should not need to repeat the cleaning and business calculations already defined here. It should be able to rely on the delivered data contract and focus on loading the validated outputs into the target warehouse. If the Load stage discovers a problem that belongs to transformation, the issue should be reported back with the affected partition and validation evidence so the team can correct and rerun Stage 2 in a controlled manner.

## 10. Acceptance Criteria and Handover to the Load Stage

The transformation stage will use a maximum rejection rate of **1% of records read** as the normal approval threshold. This limit matches the Stage 1 threshold and is intended to stop a batch when too much data is rejected, while allowing for the small number of duplicates or late-arriving records that may arise from the three-day look-back process.

Acceptance is based on the combined evidence from execution, data quality, and the output contract. A batch that has written files but failed a critical check must not be treated as ready for loading. Likewise, a batch with a non-critical warning may require a documented human decision even when its core checks pass. The purpose of the acceptance criteria is to make these decisions consistent, visible, and auditable rather than dependent on whether a run appears successful at first glance.

| Handover decision | Conditions | Required pipeline response | Decision type | Responsible role |
|---|---|---|---|---|
| `APPROVED` | Every critical TV check passes, no non-critical warning remains, and `records_rejected / records_read` is at most 1%. | Publish the transformed Parquet files, run metadata, and validation results to the Load handover location. | Automatic | Airflow transformation service |
| `MANUAL REVIEW` | All critical checks pass, but at least one non-critical warning exists, or the rejection rate is above 1% and no more than 5%. | Hold the batch. Do not pass it to Load until the Data Lead reviews and releases it. | Manual | Data Lead |
| `REJECTED` | Any critical TV check fails, or the rejection rate exceeds 5%. | Stop processing, retry up to three times, and quarantine the batch and alert an engineer if the failure continues. | Automatic stop followed by manual remediation | Data Engineering |

Retries should be reserved for failures that may be resolved by running the same task again, such as temporary storage unavailability or a transient service interruption. A deterministic data-quality failure should not be retried repeatedly without investigation because the same input and rule will normally produce the same failure. The workflow should distinguish these cases in its error categories and route them to the appropriate response.

Recovery should begin by identifying the failed task, the exact input partitions, and whether any final outputs were already published. The team should correct the underlying cause, rerun only the affected work where the plan permits it, and repeat the required validations before handover. If the process replaces a partition, the replacement should be atomic or otherwise controlled so downstream consumers cannot read a half-written result. The audit trail should preserve both the failed attempt and the successful recovery.

### Retry and Recovery Procedure

Recovery should preserve the same transformation logic and batch boundaries unless the documented issue requires a controlled change. Repeating a run with different input windows or a different rule version without recording the change can make it difficult to explain why the results differ. For that reason, the recovery record should identify the cause, corrective action, new run identifier, and final validation decision. A successful retry is acceptable only after the underlying failure has been resolved and the required checks pass.

- Airflow may make up to three retry attempts, with a ten-minute delay between attempts.
- Each retry must use the same batch window and the same idempotent output-writing method.
- A critical validation failure cannot be bypassed by retrying. The batch remains held or quarantined until the underlying issue is corrected.
- After the cause has been fixed, the corrected attempt must receive a new `run_id`. Its `batch_id` must remain the same so that downstream processing can recognize it as a rerun of the same batch window.

The handover checklist is a practical control to ensure that the receiving team has the data and the evidence needed to use it safely. The person or process completing the handover should verify that each expected file exists, can be read, matches its declared schema, and corresponds to the intended business-date range. The manifest and validation report should reference the same run identifier and output locations to avoid ambiguity when multiple runs or corrected partitions exist.

Any permitted warning should be accompanied by a concise explanation of its cause, its effect on the data, and the reason it does not prevent acceptance. A warning should never conceal an unresolved critical defect. The handover decision and its supporting evidence should be recorded in a way that can be retrieved later without reconstructing the decision from chat messages or individual recollection.

### Items Required for Load Handover

The handover package should be complete enough that the Load stage can start without asking the transformation team to recreate missing context. The data files provide the transformed records; the manifest explains which partitions and versions are included; the audit records establish lineage and processing counts; and the validation results demonstrate whether the outputs meet the acceptance criteria. If any required item is absent, the handover should be treated as incomplete even if the Parquet files themselves exist.

The transformation team must provide all of the following before the Load stage begins:

1. Partitioned Parquet files for every `analytics.dim_*` and `analytics.fact_*` dataset defined in Section 6.
2. A manifest at `data/transformed/_handover/business_date=YYYY-MM-DD/manifest.json` containing run lineage, table row counts, validation results, and quarantine totals grouped by `rule_id`.
3. Transformation audit metadata containing `run_id`, `parent_run_id`, `batch_id`, record counts, applied-rule counts, and validation results.
4. A quarantine manifest showing how many records were rejected under each rule.
5. The active `transformation_version` and the associated validation outcomes.
6. A reference to the original Stage 1 extraction run through `parent_run_id`, allowing the Load stage to trace the transformed data back to the exact input run.

The example validation summary should be used as a model for reporting, not as a source of fixed values for actual execution. Each real run must calculate its own counts, check results, and status from the records it processed. The summary should identify checks that passed, checks that generated warnings, and checks that failed. When a check is not applicable, the report should state that clearly rather than leaving the reader uncertain whether the check was omitted or forgotten.

A useful summary also connects the result to an action. For example, a failed schema check should indicate that handover is blocked pending contract review, while a permitted unknown-code warning should identify the observed count and the monitoring action required. This makes the validation report useful for operational decisions instead of serving only as a record that tests were run.

### Example of a Validation Summary

The summary is a compact view of the more detailed checks recorded for the batch. A `PASS` means the relevant control met its acceptance condition, while a `WARNING` signals an issue that does not automatically invalidate every output but may require review. The overall status must reflect the strictest applicable decision rule across the datasets. A summary should never hide a critical failure simply because most other tables passed.

| Output dataset or result | Example status |
|---|---|
| `dim_date` | PASS |
| `dim_user` | PASS |
| `dim_health_board` | WARNING — unknown-board rate is 0.09% |
| `dim_drug` | PASS |
| `dim_device_classification` | PASS |
| `fact_prescribing_monthly` | PASS |
| `fact_event_daily` | PASS |
| `fact_daily_summary` | PASS |
| Overall transformation | MANUAL REVIEW because of a non-critical warning |

This example demonstrates that a batch can pass its critical checks and still require human review because a non-critical warning remains. The overall decision must follow the documented acceptance rules rather than being selected informally.

---

The problem-and-mitigation table describes conditions that can interrupt processing or reduce the reliability of the resulting data. The response should be proportional to the problem: some conditions can be resolved automatically through deterministic rules, while others require the batch to stop and an authorized reviewer to decide what should happen. The goal is to protect the quality of the delivered dataset while minimizing unnecessary disruption to unrelated partitions.

Troubleshooting should start with the recorded evidence. Review the transformation run identifier, parent extraction run, selected input partitions, task logs, profile results, rule failures, and validation summary. This sequence helps establish whether the issue originated in the source data, the extraction handover, the transformation logic, a reference dataset, or the execution environment. Fixing the correct layer is more reliable than repeatedly rerunning the same job without understanding the cause.

When a mitigation changes processing behavior, the team should check whether the change needs to be reflected in the written plan, data contract, or unit tests. A one-time operational recovery may not require a business-rule change, but a recurring source pattern or new code value may indicate that the documented assumptions need review. The final resolution should be recorded so future runs can respond consistently and the same incident does not have to be investigated from the beginning.

## 11. Common Transformation Problems and Mitigation Strategies

The transformation process may encounter duplicate records, late data, inconsistent lookup keys, unexpected source changes, or large and uneven datasets. The following table identifies the likely causes, the risks they create, and the safeguards that should be used.

The mitigation steps should be part of routine operations wherever possible. Preventive controls, such as schema checks and deterministic deduplication, reduce the chance that an issue reaches the output stage. Monitoring and alerts help identify problems that cannot be prevented in advance, while quarantine and controlled reruns provide a way to recover without losing the evidence needed for review. After a recurring issue is resolved, the team should consider whether the rule, test, or documentation should be improved to reduce repeat incidents.

| Potential problem | Likely cause | Possible impact | Prevention or response |
|---|---|---|---|
| Duplicate rows after a rerun | Overlapping look-back partitions or a write process that is not idempotent | Daily event counts or PITC totals may be inflated | Apply deterministic deduplication under C-01 and replace only the affected partition using the stable `batch_id`. |
| Late-arriving records | The source publishes records after the initial extraction window | Daily summaries may be incomplete or out of date | Re-read the three-day trailing staging window and recalculate each affected `event_date`. |
| Late monthly PITC corrections | An NHS source file is corrected after the month has already closed | Previously calculated monthly totals may no longer be accurate | Read both the current and preceding month again, then rebuild both affected `fact_prescribing_monthly` partitions. |
| A join produces more rows than expected | `dim_drug` or `dim_health_board` contains duplicate natural keys | Measures and record counts can be multiplied | Deduplicate dimensions and enforce the join-cardinality check TV-05 before output is accepted. |
| Null DMD or BNF values create too many drug members | Each combination of null codes is incorrectly treated as a separate drug | The drug dimension can grow unnecessarily and surrogate keys may become unstable | Map valid aggregate rows to one shared `AGGREGATE` member under C-06 and B-07. |
| Unknown HBT codes appear | A new board code is introduced or the source supplies a non-standard value | Fact rows may become orphaned or be lost from reports | Retain the record, map it to the `UNKNOWN` board member, and monitor the unknown-code percentage with TV-13. |
| The staging schema changes unexpectedly | A required column is renamed, removed, or assigned a different type | The transformation may fail or interpret the data incorrectly | Run schema-contract tests before processing and stop the batch when a required contract is broken. |
| Duplicate device snapshots exist | The API is queried repeatedly or refreshed more than once in a period | Multiple current records may appear for one product | Keep the latest record using `extracted_at` or `last_updated`, then validate current-member uniqueness with TV-11. |
| API results are incomplete because of pagination limits | `API_LIMIT` is lower than the number of available results | The device dimension may not include all source products | Compare the fetched count with the API's `meta.results.total` value and raise a warning if the response appears truncated. |
| PITC aggregation runs slowly because of uneven data distribution | Some health boards or drugs have much more data than others | Monthly transformation can take longer than expected | Partition processing by `HBT` and `PaidDateMonth`, and aggregate before performing dimension joins. |

These mitigations are intended to make the transformation process repeatable and easier to troubleshoot. When a problem occurs, the run should either resolve it through a documented rule or stop and report the issue. It should not silently discard information or publish results that have failed a critical check.

A reliable recovery process also requires clear ownership. The pipeline can detect many technical conditions automatically, but a Data Lead or Data Engineering reviewer may still need to decide whether an unusual value is valid business data or a genuine defect. The resolution should be recorded alongside the affected batch so that future reviewers can see what was changed and why. This approach helps preserve consistency between the written plan, the implementation, and the final analytical outputs.

---

The final review should confirm that the transformation output is complete for the intended batch window, that the declared schemas and key rules are satisfied, and that every excluded record has a documented disposition. It should also verify that the audit metadata, operational logs, quarantine information, and manifest are available according to the retention and access requirements. These checks provide a practical final safeguard between the transformation process and downstream loading.

The plan remains limited to Stage 2. Its responsibility is to take approved staged data, apply consistent and testable transformation rules, validate the resulting analytical datasets, and deliver a traceable handover package. Source extraction remains the responsibility of Stage 1, and insertion into the target warehouse remains the responsibility of the Load stage. Maintaining this separation makes ownership clearer and helps the overall pipeline remain easier to test, troubleshoot, and extend.

## Handover Summary

The handover is the formal boundary between transformation and loading. It should identify the approved batch, list the datasets and partitions being delivered, and point to the manifest and validation evidence. This makes the result easier to audit and helps the next stage consume a stable dataset instead of repeating transformation decisions. The same process should be followed for routine batches and corrected reruns so that operational behavior remains predictable.

Stage 2 begins after the Stage 1 extraction output has been approved. It reads the staged Parquet partitions, profiles the data, applies the documented cleaning and standardization rules, derives the required business measures, and creates the `analytics` dimensional outputs. It then checks record reconciliation, key uniqueness, required fields, referential integrity, data types, business measures, and quarantine completeness before determining whether the batch can be handed over.

The transformation run preserves lineage through `parent_run_id`, records its own execution and validation details, and writes its outputs as partitioned Parquet files with a manifest for the next stage. Rejected records are quarantined with their rule identifiers and reasons, and access and retention controls protect audit information. The Load stage receives a documented, validated dataset and its contract, while the source systems are not queried again and loading into the target warehouse remains outside the scope of Stage 2.
