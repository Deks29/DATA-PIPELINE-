"""Tests for the SightSync Stage 2 transformation engine.

Run with:
    python -m unittest tests.test_transformation -v
"""

from __future__ import annotations

import json
import tempfile
import time
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

from transformation.config import TransformSettings
from transformation.metadata import TransformMetadataStore
from transformation.pipeline import (
    APPROVED,
    MANUAL_REVIEW,
    REJECTED,
    TransformationPipeline,
)

BDATE = date(2026, 10, 6)
MONTH = date(2026, 7, 1)
UTC = timezone.utc


def _ts(year: int, month: int, day: int, hour: int = 0) -> datetime:
    return datetime(year, month, day, hour, tzinfo=UTC)


class TransformationTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        root = Path(self._tmp.name)
        self.settings = TransformSettings(
            base_dir=root,
            staging_dir=root / "staging",
            transformed_dir=root / "transformed",
            quarantine_dir=root / "quarantine",
            logs_dir=root / "logs",
            metadata_db=root / "metadata" / "audit.db",
        )
        self.store = TransformMetadataStore(self.settings.metadata_db, self.settings.logs_dir)
        self.pipeline = TransformationPipeline(self.settings, self.store)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    # -- helpers --------------------------------------------------------------

    def _write(self, table: str, bdate: date, frame: pl.DataFrame) -> None:
        target = self.settings.staging_dir / table / f"business_date={bdate.isoformat()}"
        target.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(target / "data.parquet", compression="zstd")

    def _logs(self, rows: list[dict]) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "log_id": [r["log_id"] for r in rows],
                "event_type": [r.get("event_type", "view") for r in rows],
                "user_id": [r.get("user_id", "u1") for r in rows],
                "timestamp": [r["timestamp"] for r in rows],
                "payload": [r.get("payload", "{}") for r in rows],
                "run_id": [r.get("run_id", "RUN_A") for r in rows],
                "extracted_at": [r.get("extracted_at", _ts(2026, 10, 6, 6)) for r in rows],
            }
        )

    def _entities(self, rows: list[dict]) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "entity_id": [r["entity_id"] for r in rows],
                "user_id": [r.get("user_id", "u1") for r in rows],
                "status": [r.get("status", "ACTIVE") for r in rows],
                "created_at": [r["created_at"] for r in rows],
                "run_id": [r.get("run_id", "RUN_A") for r in rows],
                "extracted_at": [r.get("extracted_at", _ts(2026, 10, 6, 7)) for r in rows],
            }
        )

    def _hb(self, rows: list[dict]) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "HBT": [r.get("HBT", "S08000015") for r in rows],
                "DMDCode": [r.get("DMDCode", 1001011000001106) for r in rows],
                "BNFItemCode": [r.get("BNFItemCode", "0603020J0AAAEAE") for r in rows],
                "BNFItemDescription": [r.get("BNFItemDescription", "HYDROCORTISONE 20MG TABLETS") for r in rows],
                "PrescribedType": [r.get("PrescribedType", "VMP") for r in rows],
                "NumberOfPaidItems": [r.get("NumberOfPaidItems", 10) for r in rows],
                "PaidQuantity": [r.get("PaidQuantity", 100.0) for r in rows],
                "GrossIngredientCost": [r.get("GrossIngredientCost", 50.0) for r in rows],
                "PaidDateMonth": [r.get("PaidDateMonth", _ts(2026, 7, 1)) for r in rows],
                "run_id": [r.get("run_id", "RUN_HB") for r in rows],
                "extracted_at": [r.get("extracted_at", _ts(2026, 7, 2)) for r in rows],
            }
        )

    def _device(self, rows: list[dict] | None = None) -> pl.DataFrame:
        rows = rows or [{}]
        return pl.DataFrame(
            {
                "product_code": [r.get("product_code", "NOB") for r in rows],
                "device_name": [r.get("device_name", "Test device") for r in rows],
                "device_class": [r.get("device_class", "1") for r in rows],
                "regulation_number": [r.get("regulation_number", "880.1234") for r in rows],
                "medical_specialty_description": [r.get("medical_specialty_description", "General") for r in rows],
                "review_panel": [r.get("review_panel", "CV") for r in rows],
                "implant_flag": [r.get("implant_flag", "N") for r in rows],
                "life_sustain_support_flag": [r.get("life_sustain_support_flag", "N") for r in rows],
                "gmp_exempt_flag": [r.get("gmp_exempt_flag", "N") for r in rows],
                "third_party_flag": [r.get("third_party_flag", "N") for r in rows],
                "submission_type_id": [r.get("submission_type_id", "") for r in rows],
                "definition": [r.get("definition", "") for r in rows],
                "last_updated": [r.get("last_updated", _ts(2026, 10, 19)) for r in rows],
                "run_id": [r.get("run_id", "RUN_DEV") for r in rows],
                "extracted_at": [r.get("extracted_at", _ts(2026, 10, 19)) for r in rows],
            }
        )

    def _seed_valid(self) -> None:
        self._write(
            "raw_logs",
            BDATE,
            self._logs(
                [
                    {"log_id": "l1", "event_type": "view", "timestamp": _ts(2026, 10, 6, 1)},
                    {"log_id": "l2", "event_type": "signup", "timestamp": _ts(2026, 10, 6, 2)},
                    {"log_id": "l3", "event_type": "view", "user_id": "u2", "timestamp": _ts(2026, 10, 5, 3)},
                ]
            ),
        )
        self._write(
            "raw_entities",
            BDATE,
            self._entities(
                [{"entity_id": "e1", "created_at": _ts(2026, 10, 6, 5)}]
            ),
        )
        self._write("hb_pitc", MONTH, self._hb([{}]))
        self._write("device_classification", BDATE, self._device())

    # -- happy path -----------------------------------------------------------

    def test_happy_path_approves_and_writes_outputs(self) -> None:
        self._seed_valid()
        results = self.pipeline.run(BDATE, force=True)

        self.assertTrue(results)
        self.assertEqual({r.decision for r in results}, {APPROVED})
        by_table = {r.table_name: r for r in results}
        self.assertEqual(by_table["analytics.dim_date"].records_output, 4)
        self.assertEqual(by_table["analytics.fact_event_daily"].records_output, 3)
        self.assertEqual(by_table["analytics.fact_prescribing_monthly"].records_output, 1)

        for result in results:
            self.assertEqual(result.status, "SUCCESS")
            self.assertIsNotNone(result.output_path)
            self.assertTrue(Path(result.output_path).exists())

        dim_hb = pl.read_parquet(self.settings.transformed_dir / "dim_health_board")
        self.assertIn("S08000015", dim_hb["health_board_code"].to_list())

        fact_pres = pl.read_parquet(
            self.settings.transformed_dir / "fact_prescribing_monthly" / "business_date=2026-07-01" / "data.parquet"
        )
        self.assertEqual(fact_pres["paid_items"][0], 10)
        self.assertAlmostEqual(fact_pres["gross_ingredient_cost"][0], 50.0)

    def test_audit_rows_and_logs_written(self) -> None:
        self._seed_valid()
        results = self.pipeline.run(BDATE, force=True)
        run_id = results[0].run_id
        row = self.store.get_run(run_id, "analytics.dim_date")
        self.assertIsNotNone(row)
        self.assertEqual(row["decision"], APPROVED)
        log_files = list((self.settings.logs_dir / BDATE.isoformat()).glob("*.json"))
        self.assertEqual(len(log_files), len(results))

    def test_handover_manifest_written(self) -> None:
        self._seed_valid()
        results = self.pipeline.run(BDATE, force=True)
        manifest_path = (
            self.settings.transformed_dir
            / "_handover"
            / f"business_date={BDATE.isoformat()}"
            / "manifest.json"
        )
        self.assertTrue(manifest_path.exists())
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(manifest["decision"], APPROVED)
        self.assertEqual(manifest["batch_id"], f"{self.settings.pipeline_name}:{BDATE.isoformat()}")
        self.assertEqual(manifest["parent_run_id"], "RUN_A")
        self.assertEqual(manifest["transformation_version"], self.settings.transformation_version)
        self.assertEqual(len(manifest["tables"]), len(results))
        self.assertEqual(manifest["quarantine_manifest"], {})

    def test_dimension_and_fact_outputs_are_partitioned(self) -> None:
        self._seed_valid()
        results = self.pipeline.run(BDATE, force=True)
        by_table = {r.table_name: r for r in results}
        self.assertTrue(Path(by_table["analytics.dim_health_board"].output_path).exists())
        self.assertTrue(
            (self.settings.transformed_dir / "dim_health_board" / "business_date=2026-10-01" / "data.parquet").exists()
        )
        self.assertTrue(
            (self.settings.transformed_dir / "fact_event_daily" / "business_date=2026-10-06" / "data.parquet").exists()
        )
        self.assertTrue(
            (self.settings.transformed_dir / "fact_event_daily" / "business_date=2026-10-05" / "data.parquet").exists()
        )

    def test_write_preserves_partitions_outside_the_batch(self) -> None:
        self._seed_valid()
        historical = (
            self.settings.transformed_dir
            / "fact_event_daily"
            / "business_date=2025-01-01"
            / "data.parquet"
        )
        historical.parent.mkdir(parents=True, exist_ok=True)
        historical.write_bytes(b"historical")

        self.pipeline.run(BDATE, force=True)
        self.assertTrue(historical.exists())

    def test_rerun_without_force_keeps_successful_outputs(self) -> None:
        self._seed_valid()
        self.pipeline.run(BDATE, force=True)
        partition = (
            self.settings.transformed_dir
            / "fact_event_daily"
            / "business_date=2026-10-06"
            / "data.parquet"
        )
        first_mtime = partition.stat().st_mtime_ns
        time.sleep(0.02)

        self.pipeline.run(BDATE, force=False)
        self.assertEqual(partition.stat().st_mtime_ns, first_mtime)
        self.assertTrue(
            self.store.has_successful_run(
                f"{self.settings.pipeline_name}:{BDATE.isoformat()}",
                "analytics.fact_event_daily",
            )
        )

    # -- deduplication across partitions -------------------------------------

    def test_duplicate_logs_across_partitions_keep_latest(self) -> None:
        self._write(
            "raw_logs",
            date(2026, 10, 5),
            self._logs(
                [
                    {
                        "log_id": "dup",
                        "event_type": "view",
                        "timestamp": _ts(2026, 10, 5, 1),
                        "run_id": "RUN_OLD",
                        "extracted_at": _ts(2026, 10, 5, 22),
                    }
                ]
            ),
        )
        self._write(
            "raw_logs",
            BDATE,
            self._logs(
                [
                    {
                        "log_id": "dup",
                        "event_type": "purchase",
                        "timestamp": _ts(2026, 10, 6, 1),
                        "run_id": "RUN_NEW",
                        "extracted_at": _ts(2026, 10, 6, 22),
                    }
                ]
            ),
        )
        self._write("raw_entities", BDATE, self._entities([]))
        self._write("hb_pitc", MONTH, self._hb([{}]))
        self._write("device_classification", BDATE, self._device())

        results = self.pipeline.run(BDATE, force=True)
        by_table = {r.table_name: r for r in results}
        self.assertEqual(by_table["analytics.fact_event_daily"].records_output, 1)
        fact = pl.read_parquet(
            self.settings.transformed_dir / "fact_event_daily" / "business_date=2026-10-06" / "data.parquet"
        )
        self.assertEqual(fact["event_type"].to_list(), ["purchase"])

    # -- quarantine / rejection ----------------------------------------------

    def test_negative_measure_is_quarantined_and_rejected(self) -> None:
        self._write("raw_logs", BDATE, self._logs([{"log_id": "l1", "timestamp": _ts(2026, 10, 6, 1)}]))
        self._write("raw_entities", BDATE, self._entities([]))
        self._write(
            "hb_pitc",
            MONTH,
            self._hb([{"GrossIngredientCost": -10.0}]),
        )
        self._write("device_classification", BDATE, self._device())

        results = self.pipeline.run(BDATE, force=True)
        self.assertEqual({r.decision for r in results}, {REJECTED})

        quarantine = self.settings.quarantine_dir / "hb_pitc" / f"business_date={BDATE.isoformat()}" / "rejected.parquet"
        self.assertTrue(quarantine.exists())
        rejected = pl.read_parquet(quarantine)
        self.assertEqual(rejected.height, 1)
        self.assertEqual(rejected["rule_id"][0], "C-07")
        self.assertEqual(
            rejected["batch_id"][0], f"{self.settings.pipeline_name}:{BDATE.isoformat()}"
        )
        self.assertEqual(rejected["parent_run_id"][0], "RUN_HB")

    # -- business rules -------------------------------------------------------

    def test_invalid_health_board_maps_to_unknown(self) -> None:
        self._write("raw_logs", BDATE, self._logs([{"log_id": "l1", "timestamp": _ts(2026, 10, 6, 1)}]))
        self._write("raw_entities", BDATE, self._entities([]))
        self._write("hb_pitc", MONTH, self._hb([{"HBT": "ZZZ"}]))
        self._write("device_classification", BDATE, self._device())

        results = self.pipeline.run(BDATE, force=True)
        by_table = {r.table_name: r for r in results}
        self.assertIn(by_table["analytics.fact_prescribing_monthly"].decision, (APPROVED, MANUAL_REVIEW))

        dim_hb = pl.read_parquet(self.settings.transformed_dir / "dim_health_board")
        self.assertIn("UNKNOWN", dim_hb["health_board_code"].to_list())

    def test_zero_quantity_flagged(self) -> None:
        self._write("raw_logs", BDATE, self._logs([{"log_id": "l1", "timestamp": _ts(2026, 10, 6, 1)}]))
        self._write("raw_entities", BDATE, self._entities([]))
        self._write(
            "hb_pitc",
            MONTH,
            self._hb([{"PaidQuantity": 0.0, "NumberOfPaidItems": 5}]),
        )
        self._write("device_classification", BDATE, self._device())

        results = self.pipeline.run(BDATE, force=True)
        by_table = {r.table_name: r for r in results}
        self.assertEqual(by_table["analytics.fact_prescribing_monthly"].decision, APPROVED)
        fact = pl.read_parquet(
            self.settings.transformed_dir / "fact_prescribing_monthly" / "business_date=2026-07-01" / "data.parquet"
        )
        self.assertTrue(bool(fact["zero_quantity_flag"][0]))

    # -- retention ------------------------------------------------------------

    def test_purge_removes_expired_quarantine_partitions(self) -> None:
        self.store.quarantine_dir = self.settings.quarantine_dir
        old = self.settings.quarantine_dir / "hb_pitc" / "business_date=2000-01-01"
        recent = self.settings.quarantine_dir / "hb_pitc" / f"business_date={BDATE.isoformat()}"
        old.mkdir(parents=True, exist_ok=True)
        recent.mkdir(parents=True, exist_ok=True)
        (old / "rejected.parquet").write_bytes(b"old")
        (recent / "rejected.parquet").write_bytes(b"recent")

        removed = self.store.purge_expired(log_retention_days=90, audit_retention_months=24)
        self.assertEqual(removed["quarantine_partitions_removed"], 1)
        self.assertFalse(old.exists())
        self.assertTrue(recent.exists())

    # -- date resolution ------------------------------------------------------

    def test_default_business_date_uses_latest_partition(self) -> None:
        self._write("raw_logs", BDATE, self._logs([{"log_id": "l1", "timestamp": _ts(2026, 10, 6, 1)}]))
        self._write("raw_entities", BDATE, self._entities([]))
        self._write("hb_pitc", MONTH, self._hb([{}]))
        self._write("device_classification", BDATE, self._device())

        results = self.pipeline.run(None, force=True)
        self.assertTrue(results)
        self.assertEqual(self.pipeline._resolve_business_date(None), MONTH)


if __name__ == "__main__":
    unittest.main()
