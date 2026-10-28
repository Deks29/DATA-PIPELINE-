"""Command line entry point for the SightSync Stage 2 transformation."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime

from transformation.pipeline import APPROVED, APPROVED_WITH_WARNING, MANUAL_REVIEW, TransformationPipeline


def _parse_date(value: str):
    return datetime.strptime(value, "%Y-%m-%d").date()


def _summarise(results, business_date) -> dict:
    checks = results[0].checks if results else []
    decision = results[0].decision if results else "REJECTED"
    return {
        "run_id": results[0].run_id if results else None,
        "business_date": business_date.isoformat() if business_date else None,
        "decision": decision,
        "checks": [
            {
                "id": c.check_id,
                "name": c.name,
                "target": c.target,
                "severity": c.severity,
                "status": c.status,
                "detail": c.detail,
            }
            for c in checks
        ],
        "tables": [
            {
                "table": r.table_name,
                "status": r.status,
                "records_read": r.records_read,
                "records_output": r.records_output,
                "records_rejected": r.records_rejected,
                "output_path": r.output_path,
                "quarantine_path": r.quarantine_path,
            }
            for r in results
        ],
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="SightSync Stage 2 transformation")
    parser.add_argument("--business-date", help="Business date YYYY-MM-DD (defaults to latest staged partition)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing transformed outputs")
    parser.add_argument("--json", action="store_true", dest="json_output", help="Emit machine readable JSON")
    args = parser.parse_args(argv)

    business_date = _parse_date(args.business_date) if args.business_date else None

    pipeline = TransformationPipeline()
    try:
        results = pipeline.run(business_date=business_date, force=args.force)
    except Exception as exc:  # pragma: no cover - surfaced to the operator
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1

    effective_date = business_date or pipeline._resolve_business_date(None)
    summary = _summarise(results, effective_date)
    decision = summary["decision"]

    if args.json_output:
        print(json.dumps(summary, indent=2))
    else:
        print(f"[{decision}] run_id={summary['run_id']} business_date={summary['business_date']}")
        for check in summary["checks"]:
            print(f"  {check['id']} {check['status']} {check['name']} [{check['target']}] - {check['detail']}")
        print("Tables:")
        for table in summary["tables"]:
            print(
                f"  {table['table']}: {table['status']} "
                f"read={table['records_read']} out={table['records_output']} "
                f"rejected={table['records_rejected']}"
            )
            if table["output_path"]:
                print(f"    output: {table['output_path']}")
            if table["quarantine_path"]:
                print(f"    quarantine: {table['quarantine_path']}")

    if decision == "REJECTED":
        return 1
    if decision == MANUAL_REVIEW:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
