"""Tests for the openFDA API source used by the extraction stage."""

from __future__ import annotations

import tempfile
import unittest
import urllib.parse
from pathlib import Path
from unittest import mock

from extraction.api import (
    build_url,
    fetch_records,
    fetch_to_ndjson,
    normalise_records,
)


class BuildUrlTests(unittest.TestCase):
    def test_search_and_limit_are_added(self):
        url = build_url("https://api.fda.gov/device/classification.json", "product_code:NOB", 500)
        params = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        self.assertEqual(params["search"], ["product_code:NOB"])
        self.assertEqual(params["limit"], ["500"])

    def test_existing_query_is_preserved(self):
        url = build_url("https://api.fda.gov/x.json?search=product_code:NOB", limit=10, skip=20)
        params = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        self.assertEqual(params["search"], ["product_code:NOB"])
        self.assertEqual(params["skip"], ["20"])


class FetchRecordsTests(unittest.TestCase):
    @mock.patch("extraction.api._get_json")
    def test_paginates_until_total(self, get_json):
        get_json.side_effect = [
            {"meta": {"results": {"total": 3}}, "results": [{"product_code": "A"}]},
            {"meta": {"results": {"total": 3}}, "results": [{"product_code": "B"}, {"product_code": "C"}]},
        ]
        records, meta = fetch_records("https://api.fda.gov/device/classification.json", limit=1)
        self.assertEqual([r["product_code"] for r in records], ["A", "B", "C"])
        self.assertEqual(meta["results"]["total"], 3)
        self.assertEqual(get_json.call_count, 2)


class NormaliseTests(unittest.TestCase):
    def test_missing_fields_become_empty_strings(self):
        rows = normalise_records([{"product_code": "NOB"}], ["product_code", "definition"], "2026-10-19")
        self.assertEqual(rows[0], {"product_code": "NOB", "definition": "", "last_updated": "2026-10-19"})


class FetchToNdjsonTests(unittest.TestCase):
    def test_writes_ndjson(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "device_classification.json"
            with mock.patch("extraction.api.fetch_records", return_value=([{"product_code": "NOB"}], {"last_updated": "2026-10-19"})):
                path, count, _ = fetch_to_ndjson(
                    "https://api.fda.gov/device/classification.json",
                    out,
                    ["product_code", "definition"],
                )
            self.assertEqual(path, out)
            self.assertEqual(count, 1)
            self.assertIn('"last_updated": "2026-10-19"', out.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
