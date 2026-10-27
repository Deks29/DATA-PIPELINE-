"""Generate sample raw CSV/JSON log drops for local development and tests.

Usage:
    python scripts/generate_sample_data.py [--business-date 2026-10-06]
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import random
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"

EVENT_TYPES = ["click", "view", "purchase", "signup", "logout"]
STATUSES = ["active", "completed", "cancelled", "pending"]


def _ts(day: date, hour: int, minute: int, second: int = 0) -> str:
    return datetime.combine(day, time(hour, minute, second), tzinfo=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def generate(business_date: date, log_rows: int = 500, entity_rows: int = 200) -> None:
    logs_dir = RAW_DIR / "raw_logs"
    entities_dir = RAW_DIR / "raw_entities"
    logs_dir.mkdir(parents=True, exist_ok=True)
    entities_dir.mkdir(parents=True, exist_ok=True)

    rng = random.Random(42)

    # --- raw_logs: primary CSV drop for the business date --------------------
    log_rows_data: list[list[str]] = []
    for i in range(log_rows):
        hour = rng.randint(0, 23)
        minute = rng.randint(0, 59)
        user = f"user_{rng.randint(1000, 1999)}"
        payload = json.dumps({"page": f"/p/{rng.randint(1, 50)}", "device": rng.choice(["ios", "android", "web"])})
        log_rows_data.append(
            [
                f'log_{business_date.strftime("%Y%m%d")}_{i:05d}',
                rng.choice(EVENT_TYPES),
                user,
                _ts(business_date, hour, minute),
                payload,
            ]
        )
    # a few late-arriving logs from the trailing look-back window
    for i in range(5):
        day = business_date - timedelta(days=1)
        log_rows_data.append(
            [
                f"log_late_{i:03d}",
                "view",
                f"user_late_{i}",
                _ts(day, rng.randint(0, 23), rng.randint(0, 59)),
                '{"late":true}',
            ]
        )
    _write_csv(logs_dir / f"raw_logs_{business_date.isoformat()}.csv", ["log_id", "event_type", "user_id", "timestamp", "payload"], log_rows_data)

    # --- raw_logs: secondary JSON drop (same business date) ------------------
    json_records = []
    for i in range(20):
        json_records.append(
            {
                "log_id": f"log_json_{i:03d}",
                "event_type": rng.choice(EVENT_TYPES),
                "user_id": f"user_{rng.randint(1000, 1999)}",
                "timestamp": _ts(business_date, rng.randint(0, 23), rng.randint(0, 59)),
                "payload": json.dumps({"source": "json_drop"}),
            }
        )
    (logs_dir / f"raw_logs_{business_date.isoformat()}_json.json").write_text(
        "\n".join(json.dumps(r) for r in json_records) + "\n", encoding="utf-8"
    )

    # --- raw_entities: transactional metadata --------------------------------
    ent_rows_data: list[list[str]] = []
    for i in range(entity_rows):
        ent_rows_data.append(
            [
                f"entity_{i:05d}",
                f"user_{rng.randint(1000, 1999)}",
                rng.choice(STATUSES),
                _ts(business_date, rng.randint(0, 23), rng.randint(0, 59)),
            ]
        )
    _write_csv(entities_dir / f"raw_entities_{business_date.isoformat()}.csv", ["entity_id", "user_id", "status", "created_at"], ent_rows_data)

    print(f"Sample data written for {business_date} -> {RAW_DIR}")


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    path.write_text(buffer.getvalue(), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate SightSync sample raw data")
    parser.add_argument("--business-date", default="2026-10-06")
    parser.add_argument("--log-rows", type=int, default=500)
    parser.add_argument("--entity-rows", type=int, default=200)
    args = parser.parse_args()
    generate(date.fromisoformat(args.business_date), args.log_rows, args.entity_rows)


if __name__ == "__main__":
    main()
