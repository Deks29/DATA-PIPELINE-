"""Unit and integration tests for the SightSync Stage 1 extraction engine.

Run with:
    python -m unittest discover -s tests -v
"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, datetime, time, timezone
from pathlib import Path

import polars as pl

from extraction.config import Settings
from extraction.metadata import MetadataStore
from extraction.pipeline import (
    APPROVED,
    APPROVED_WITH_WARNING,
    MANUAL_REVIEW,
    REJECTED,
    ExtractionPipeline,
    decide,
)
from extraction.validations import (
    CRITICAL,
    FAIL,
    PASS,
    WARNING,
    CheckResult,
    check_parquet_parity,
    check_primary_key_uniqueness,
)

BDATE = date(2026, 10, 6)
WINDOW_START = datetime(2026, 10, 3, 0, 0, 0, tzinfo=timezone.utc)


def _result(check_id: str, status: str, severity: str) -> CheckResult:
    return CheckResult(check_id, check_id, "t", "c", severity, status)


class DecisionTests(unittest.TestCase):
    def test_approved(self):
        checks = [_result("V1", PASS, CRITICAL), _result("V7", PASS, WARNING)]
        self.assertEqual(decide(checks, 100, 0, 0.01), APPROVED)

    def test_approved_with_warning(self):
        checks = [_result("V1", PASS, CRITICAL), _result("V7", FAIL, WARNING)]
        self.assertEqual(decide(checks, 100, 0, 0.01), APPROVED_WITH_WARNING)

    def test_rejected_on_critical_fail(self):
        checks = [_result("V4", FAIL, CRITICAL), _result("V7", FAIL, WARNING)]
        self.assertEqual(decide(checks, 100, 0, 0.01), REJECTED)

    def test_manual_review_on_high_rejection_rate(self):
        checks = [_result("V1", PASS, CRITICAL), _result("V7", PASS, WARNING)]
        self.assertEqual(decide(checks, 100, 5, 0.01), MANUAL_REVIEW)


class ValidatorTests(unittest.TestCase):
    def test_pk_uniqueness_fail(self):
        df = pl.DataFrame({"log_id": ["a", "a", "b"]})
        self.assertEqual(check_primary_key_uniqueness(df, "log_id").status, FAIL)

    def test_parquet_parity_ok(self):
        self.assertEqual(check_parquet_parity(10, 9, 1, 9).status, PASS)

    def test_parquet_parity_write_mismatch(self):
        self.assertEqual(check_parquet_parity(10, 9, 1, 8).status, FAIL)


class PipelineTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        root = Path(self._tmp.name)
        self.settings = Settings(
            base_dir=root,
            raw_dir=root / "raw",
            staging_dir=root / "staging",
            logs_dir=root / "logs",
            metadata_db=root / "metadata" / "audit.db",
        )
        self.store = MetadataStore(self.settings.metadata_db, self.settings.logs_dir)
        self.pipeline = ExtractionPipeline(self.settings, self.store)

    def tearDown(self):
        self._tmp.cleanup()

    # -- helpers --------------------------------------------------------------

    def _write_logs(self, rows: list[dict], name: str = "raw_logs_2026-10-06.csv") -> None:
        d = self.settings.raw_dir_for("raw_logs")
        d.mkdir(parents=True, exist_ok=True)
        if name.endswith(".csv"):
            text = "log_id,event_type,user_id,timestamp,payload\n"
            text += "\n".join(
                f'{r["log_id"]},{r["event_type"]},{r["user_id"]},{r["timestamp"]},{r["payload"]}'
                for r in rows
            )
            (d / name).write_text(text + "\n", encoding="utf-8")
        else:
            (d / name).write_text(
                "\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8"
            )

    def _write_entities(self, rows: list[dict]) -> None:
        d = self.settings.raw_dir_for("raw_entities")
        d.mkdir(parents=True, exist_ok=True)
        text = "entity_id,user_id,status,created_at\n"
        text += "\n".join(
            f'{r["entity_id"]},{r["user_id"]},{r["status"]},{r["created_at"]}' for r in rows
        )
        (d / "raw_entities_2026-10-06.csv").write_text(text + "\n", encoding="utf-8")

    @staticmethod
    def _log(log_id: str, ts: str) -> dict:
        return {
            "log_id": log_id,
            "event_type": "view",
            "user_id": "user_1",
            "timestamp": ts,
            "payload": "{}",
        }

    # -- happy path -----------------------------------------------------------

    def test_happy_path_writes_partitioned_parquet(self):
        self._write_logs(
            [
                self._log("l1", "2026-10-06T01:00:00Z"),
                self._log("l2", "2026-10-06T23:59:59Z"),
                self._log("late", "2026-10-04T12:00:00Z"),
                self._log("old", "2026-09-01T12:00:00Z"),  # outside window
            ]
        )
        self._write_entities(
            [{"entity_id": "e1", "user_id": "user_1", "status": "active", "created_at": "2026-10-06T05:00:00Z"}]
        )

        results = self.pipeline.run(BDATE, tables=["raw_logs", "raw_entities"])
        by_table = {r.table_key: r for r in results}

        self.assertEqual(by_table["raw_logs"].decision, APPROVED)
        self.assertEqual(by_table["raw_logs"].rows_extracted, 3)
        self.assertEqual(by_table["raw_logs"].rows_rejected, 0)
        self.assertEqual(by_table["raw_entities"].decision, APPROVED)

        parquet = Path(by_table["raw_logs"].parquet_path)
        self.assertTrue(parquet.exists())
        self.assertEqual(parquet.parent.name, "business_date=2026-10-06")
        out = pl.read_parquet(parquet)
        for control in ("run_id", "business_date", "extracted_at"):
            self.assertIn(control, out.columns)

    def test_json_source_is_supported(self):
        self._write_logs(
            [self._log("j1", "2026-10-06T10:00:00Z"), self._log("j2", "2026-10-06T11:00:00Z")],
            name="raw_logs_2026-10-06.json",
        )
        result = self.pipeline.extract(BDATE, "raw_logs")
        self.assertEqual(result.decision, APPROVED)
        self.assertEqual(result.rows_extracted, 2)

    # -- failure paths --------------------------------------------------------

    def test_missing_files_rejected(self):
        result = self.pipeline.extract(BDATE, "raw_logs")
        self.assertEqual(result.decision, REJECTED)
        self.assertEqual(result.check_map["V1"], FAIL)

    def test_null_primary_key_rejected(self):
        self._write_logs(
            [self._log("l1", "2026-10-06T01:00:00Z"), self._log("", "2026-10-06T02:00:00Z")]
        )
        result = self.pipeline.extract(BDATE, "raw_logs")
        # Empty CSV string is read as null -> V4 fails, extraction rejected.
        self.assertEqual(result.decision, REJECTED)
        self.assertEqual(result.check_map["V4"], FAIL)

    def test_schema_drift_missing_column_rejected(self):
        d = self.settings.raw_dir_for("raw_logs")
        d.mkdir(parents=True, exist_ok=True)
        (d / "raw_logs_2026-10-06.csv").write_text(
            "log_id,event_type,user_id\nl1,view,user_1\n", encoding="utf-8"
        )
        result = self.pipeline.extract(BDATE, "raw_logs")
        self.assertEqual(result.decision, REJECTED)
        self.assertEqual(result.check_map["V2"], FAIL)

    def test_idempotent_skip_on_second_run(self):
        self._write_logs([self._log("l1", "2026-10-06T01:00:00Z")])
        first = self.pipeline.extract(BDATE, "raw_logs")
        self.assertEqual(first.decision, APPROVED)
        second = self.pipeline.extract(BDATE, "raw_logs")
        self.assertEqual(second.decision, "SKIPPED")

    def test_force_overwrites_previous_run(self):
        self._write_logs([self._log("l1", "2026-10-06T01:00:00Z")])
        self.pipeline.extract(BDATE, "raw_logs")
        forced = self.pipeline.extract(BDATE, "raw_logs", force=True)
        self.assertEqual(forced.decision, APPROVED)

    def test_audit_record_and_json_log_written(self):
        self._write_logs([self._log("l1", "2026-10-06T01:00:00Z")])
        result = self.pipeline.extract(BDATE, "raw_logs")
        row = self.store.get_run(result.run_id, "staging.raw_logs")
        self.assertIsNotNone(row)
        self.assertEqual(row["decision"], APPROVED)
        log_files = list((self.settings.logs_dir / BDATE.isoformat()).glob("*.json"))
        self.assertEqual(len(log_files), 1)

    # -- HB PITC -----------------------------------------------------------------

    def _write_prescriptions(self, rows: list[tuple]) -> None:
        d = self.settings.raw_dir_for("hb_pitc")
        d.mkdir(parents=True, exist_ok=True)
        header = (
            "HBT,DMDCode,BNFItemCode,BNFItemDescription,PrescribedType,"
            "NumberOfPaidItems,PaidQuantity,GrossIngredientCost,PaidDateMonth\n"
        )
        text = "\n".join(",".join(str(value) for value in row) for row in rows)
        (d / "hb_pitc2026_07.csv").write_text(header + text + "\n", encoding="utf-8")

    def test_hb_pitc_month_window_and_composite_key(self):
        self._write_prescriptions(
            [
                (
                    "S08000015", "1001011000001106", "0603020J0AAAEAE",
                    "HYDROCORTISONE 20MG TABLETS", "VMP", 31, 1394, 179.42, 202607,
                ),
                ("S08000015", "", "", "", "AMP", 35, 9743.5, 7276.7, 202607),
                (
                    "S08000015", "1001011000001106", "0603020J0AAAEAE",
                    "HYDROCORTISONE 20MG TABLETS", "VMP", 10, 500, 50.0, 202606,
                ),
            ]
        )
        result = self.pipeline.extract(date(2026, 7, 1), "hb_pitc")
        self.assertEqual(result.decision, APPROVED)
        self.assertEqual(result.rows_read, 2)
        self.assertEqual(result.rows_extracted, 2)
        self.assertEqual(result.check_map["V4"], PASS)
        self.assertEqual(result.check_map["V5"], PASS)

        parquet = Path(result.parquet_path)
        self.assertEqual(parquet.parent.name, "business_date=2026-07-01")
        out = pl.read_parquet(parquet)
        self.assertEqual(out["PaidDateMonth"].unique().to_list(), [datetime(2026, 7, 1, tzinfo=timezone.utc)])

    def test_hb_pitc_duplicate_composite_key_rejected(self):
        row = (
            "S08000015", "1001011000001106", "0603020J0AAAEAE",
            "HYDROCORTISONE 20MG TABLETS", "VMP", 31, 1394, 179.42, 202607,
        )
        self._write_prescriptions([row, row])
        result = self.pipeline.extract(date(2026, 7, 1), "hb_pitc")
        self.assertEqual(result.decision, REJECTED)
        self.assertEqual(result.check_map["V5"], FAIL)


if __name__ == "__main__":
    unittest.main()
