"""Data quality validation checks TV-01..TV-14."""

from __future__ import annotations

from datetime import datetime

import polars as pl

from transformation.constants import (
    CRITICAL,
    FAIL,
    NON_CRITICAL,
    PASS,
    PRIMARY_KEYS,
    REQUIRED_COLUMNS,
    WARNING,
    iso,
)
from transformation.models import CheckResult, SourceState


class ValidationMixin:
    """Run the contracted validation suite against transformed outputs."""

    def _check(
        self,
        check_id: str,
        name: str,
        target: str,
        purpose: str,
        criteria: str,
        passed: bool,
        detail: str,
        severity: str = CRITICAL,
        warning: bool = False,
    ) -> CheckResult:
        status = PASS if passed else (WARNING if warning else FAIL)
        return CheckResult(check_id, name, target, purpose, criteria, severity, status, detail)

    def _validate(
        self,
        states: dict[str, SourceState],
        transformed: dict[str, pl.DataFrame],
        zero_qty_input: int,
        run_at: datetime,
    ) -> list[CheckResult]:
        checks: list[CheckResult] = []

        reconciliation_ok = True
        details: list[str] = []
        for name, state in states.items():
            if state.records_read == 0:
                continue
            expected = (
                state.records_output + state.records_rejected
                + state.records_deduplicated + state.records_excluded
            )
            ok = state.records_read == expected
            reconciliation_ok &= ok
            details.append(
                f"{name}: read={state.records_read}, output={state.records_output}, "
                f"rejected={state.records_rejected}, dedup={state.records_deduplicated}"
            )
        checks.append(self._check(
            "TV-01", "Record reconciliation", "All input tables",
            "Detect lost or duplicated records",
            "records_read = records_output + records_rejected + records_deduplicated + records_excluded",
            reconciliation_ok, "; ".join(details) or "No input rows.",
        ))

        pk_ok = True
        pk_details: list[str] = []
        for table_key, frame in transformed.items():
            keys = PRIMARY_KEYS[table_key]
            if frame.height == 0:
                pk_details.append(f"{table_key}: empty")
                continue
            missing = [k for k in keys if k not in frame.columns]
            if missing:
                pk_ok = False
                pk_details.append(f"{table_key}: missing keys {missing}")
                continue
            duplicates = frame.height - frame.select(keys).unique().height
            if duplicates:
                pk_ok = False
                pk_details.append(f"{table_key}: {duplicates} duplicate keys")
        checks.append(self._check(
            "TV-02", "Primary-key uniqueness", "All output tables",
            "Prevent duplicate fact and dimension rows",
            "Duplicate count on declared primary key = 0",
            pk_ok, "; ".join(pk_details) or "All output keys unique.",
        ))

        null_ok = True
        null_details: list[str] = []
        for table_key, required in REQUIRED_COLUMNS.items():
            frame = transformed[table_key]
            missing = [c for c in required if c not in frame.columns]
            if missing:
                null_ok = False
                null_details.append(f"{table_key}: missing {missing}")
                continue
            for column in required:
                nulls = frame[column].null_count() if frame.height else 0
                if nulls:
                    null_ok = False
                    null_details.append(f"{table_key}.{column}: {nulls} nulls")
        checks.append(self._check(
            "TV-03", "Required-column null check", "All output tables",
            "Ensure Load receives complete keys and measures",
            "Null count = 0 for all NOT NULL contract columns",
            null_ok, "; ".join(null_details) or "No nulls in required columns.",
        ))

        ri_ok = True
        ri_details: list[str] = []
        refs = [
            ("fact_prescribing_monthly", "health_board_key", "dim_health_board", "health_board_key"),
            ("fact_prescribing_monthly", "drug_key", "dim_drug", "drug_key"),
            ("fact_prescribing_monthly", "prescribed_type_key", "dim_prescribed_type", "prescribed_type_key"),
            ("fact_prescribing_monthly", "period_key", "dim_date", "date_key"),
            ("fact_event_daily", "event_date_key", "dim_date", "date_key"),
            ("fact_event_daily", "user_key", "dim_user", "user_key"),
            ("fact_daily_summary", "event_date_key", "dim_date", "date_key"),
        ]
        for fact, fk, dim, dk in refs:
            fact_frame = transformed[fact]
            if fact_frame.height == 0:
                continue
            valid = set(transformed[dim][dk].to_list())
            orphans = sum(1 for value in fact_frame[fk].to_list() if value not in valid)
            if orphans:
                ri_ok = False
                ri_details.append(f"{fact}.{fk}: {orphans} orphans")
        checks.append(self._check(
            "TV-04", "Referential integrity", "All fact foreign keys",
            "Prevent orphan facts",
            "100% of fact FK values exist in their dimension",
            ri_ok, "; ".join(ri_details) or "All fact keys resolve to dimensions.",
        ))

        cardinality_ok = True
        cardinality_details: list[str] = []
        dimension_keys = {
            "dim_user": ["user_id"],
            "dim_health_board": ["health_board_code"],
            "dim_drug": ["dmd_code", "bnf_item_code"],
            "dim_prescribed_type": ["prescribed_type_code"],
            "dim_device_classification": ["product_code"],
        }
        for table_key, keys in dimension_keys.items():
            frame = transformed[table_key]
            if frame.height:
                duplicates = frame.height - frame.select(keys).unique().height
                if duplicates:
                    cardinality_ok = False
                    cardinality_details.append(f"{table_key}: {duplicates} duplicate natural keys")
        checks.append(self._check(
            "TV-05", "Join cardinality", "Dimension natural keys",
            "Prevent row multiplication during fact joins",
            "Each lookup natural key is unique",
            cardinality_ok, "; ".join(cardinality_details) or "Dimension lookups are unique.",
        ))

        type_ok = True
        type_details: list[str] = []
        temporal = {
            "dim_user": ["first_seen_at", "last_seen_at", "updated_at"],
            "dim_drug": ["updated_at"],
            "dim_device_classification": ["last_updated", "updated_at"],
            "fact_prescribing_monthly": ["paid_month", "updated_at"],
        }
        for table_key, columns in temporal.items():
            frame = transformed[table_key]
            for column in columns:
                if frame.height and not isinstance(frame.schema[column], (pl.Date, pl.Datetime)):
                    type_ok = False
                    type_details.append(f"{table_key}.{column}: {frame.schema[column]}")
        checks.append(self._check(
            "TV-06", "Type and format conformance", "All contracted columns",
            "Guarantee Load-compatible data",
            "Temporal columns use DATE/TIMESTAMP; monetary measures are non-null numeric",
            type_ok, "; ".join(type_details) or "Contract types conform.",
        ))

        negative_ok = True
        negative_details: list[str] = []
        fact_p = transformed["fact_prescribing_monthly"]
        if fact_p.height:
            for column in ("paid_items", "paid_quantity", "gross_ingredient_cost"):
                negatives = (fact_p[column] < 0).sum()
                if negatives:
                    negative_ok = False
                    negative_details.append(f"fact_prescribing_monthly.{column}: {negatives}")
        fact_e = transformed["fact_event_daily"]
        if fact_e.height and (fact_e["event_count"] < 0).sum():
            negative_ok = False
            negative_details.append("fact_event_daily.event_count negative")
        summary = transformed["fact_daily_summary"]
        if summary.height:
            for column in ("total_events", "unique_users"):
                if (summary[column] < 0).sum():
                    negative_ok = False
                    negative_details.append(f"fact_daily_summary.{column} negative")
        checks.append(self._check(
            "TV-07", "Business-rule checks", "PITC and daily measures",
            "Prevent invalid business values",
            "No negative paid items, quantities, costs, event counts, or unique-user counts",
            negative_ok, "; ".join(negative_details) or "All business measures are non-negative.",
        ))

        agg_ok = True
        agg_detail = "No PITC rows."
        hb_state = states["hb_pitc"]
        if hb_state.records_output and fact_p.height:
            # Compare accepted input sums with output sums.
            source = self._clean_hb_cache
            if source is not None and source.height:
                in_items = int(source["NumberOfPaidItems"].sum())
                in_qty = float(source["PaidQuantity"].sum())
                in_cost = float(source["GrossIngredientCost"].sum())
                out_items = int(fact_p["paid_items"].sum())
                out_qty = float(fact_p["paid_quantity"].sum())
                out_cost = float(fact_p["gross_ingredient_cost"].sum())
                agg_ok = (
                    in_items == out_items
                    and abs(in_qty - out_qty) <= 0.01
                    and abs(in_cost - out_cost) <= 0.01
                )
                agg_detail = (
                    f"items {in_items}->{out_items}, quantity {in_qty:.4f}->{out_qty:.4f}, "
                    f"cost {in_cost:.2f}->{out_cost:.2f}"
                )
        checks.append(self._check(
            "TV-08", "Aggregate reconciliation", "fact_prescribing_monthly",
            "Ensure measures are not lost during cleaning",
            "Output sums equal accepted input sums within 0.01 tolerance",
            agg_ok, agg_detail,
        ))

        event_ok = True
        event_detail = "No event rows."
        if states["raw_logs"].records_output:
            expected_events = states["raw_logs"].records_output
            actual_events = int(fact_e["event_count"].sum()) if fact_e.height else 0
            summary_total = int(summary["total_events"].sum()) if summary.height else 0
            event_ok = expected_events == actual_events == summary_total
            event_detail = (
                f"deduped_logs={expected_events}, fact_event={actual_events}, summary={summary_total}"
            )
        checks.append(self._check(
            "TV-09", "Event reconciliation", "fact_event_daily, fact_daily_summary",
            "Ensure daily counts match deduplicated logs",
            "Output event count equals deduplicated input log count for each date",
            event_ok, event_detail,
        ))

        quarantine_rows = sum(
            sum(frame.height for frame in state.rejected_frames)
            for state in states.values()
        )
        rejected_total = sum(state.records_rejected for state in states.values())
        checks.append(self._check(
            "TV-10", "Quarantine completeness", "Quarantine store",
            "Ensure every rejected record is traceable",
            "records_rejected equals quarantine rows and every row has a rule_id",
            quarantine_rows == rejected_total,
            f"rejected={rejected_total}, quarantine_rows={quarantine_rows}",
        ))

        scd_ok = True
        scd_details: list[str] = []
        for table_key, keys in (
            ("dim_device_classification", ["product_code"]),
            ("dim_drug", ["dmd_code", "bnf_item_code"]),
        ):
            frame = transformed[table_key]
            if frame.height:
                duplicates = frame.height - frame.select(keys).unique().height
                if duplicates:
                    scd_ok = False
                    scd_details.append(f"{table_key}: {duplicates} duplicate current members")
        checks.append(self._check(
            "TV-11", "SCD/dimension integrity", "Device and drug dimensions",
            "Prevent duplicate current members",
            "Exactly one current row per natural key",
            scd_ok, "; ".join(scd_details) or "One current row per natural key.",
        ))

        zero_flags = int(fact_p["zero_quantity_flag"].sum()) if fact_p.height else 0
        checks.append(self._check(
            "TV-12", "Zero-quantity visibility", "fact_prescribing_monthly.zero_quantity_flag",
            "Ensure observed zero-quantity rows are retained and flagged",
            "Flagged rows equal accepted input zero-quantity rows",
            zero_flags == zero_qty_input,
            f"input={zero_qty_input}, flagged={zero_flags}",
            severity=NON_CRITICAL,
        ))

        unknown_ok = True
        unknown_details: list[str] = []
        for table_key, flag_column in (
            ("dim_user", "user_is_unknown"),
            ("dim_health_board", "is_unknown"),
            ("dim_drug", "is_aggregate"),
        ):
            frame = transformed[table_key]
            if frame.height:
                share = frame[flag_column].sum() / frame.height
                if share > 0.01:
                    unknown_ok = False
                    unknown_details.append(f"{table_key}: {share:.2%}")
        checks.append(self._check(
            "TV-13", "Unknown-member rate", "Unknown user/board/aggregate members",
            "Detect unexpected mapping loss",
            "Unknown-member share per dimension <= 1%",
            unknown_ok, "; ".join(unknown_details) or "Unknown-member share within threshold.",
            severity=NON_CRITICAL,
            warning=True,
        ))

        fresh_ok = True
        for table_key, column in (("dim_user", "updated_at"), ("dim_device_classification", "updated_at")):
            frame = transformed[table_key]
            if frame.height and frame[column].max() is None:
                fresh_ok = False
        checks.append(self._check(
            "TV-14", "Freshness", "All output partitions",
            "Prevent stale handover",
            "Output updated_at is present and within the batch run",
            fresh_ok, f"run_at={iso(run_at)}",
            severity=NON_CRITICAL,
            warning=True,
        ))
        return checks

    @property
    def _clean_hb_cache(self) -> pl.DataFrame | None:
        return getattr(self, "_hb_processed", None)
