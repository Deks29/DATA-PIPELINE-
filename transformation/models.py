"""Result and state models shared across the Stage 2 transformation."""

from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl

from transformation.constants import APPROVED, APPROVED_WITH_WARNING


@dataclass
class CheckResult:
    check_id: str
    name: str
    target: str
    purpose: str
    criteria: str
    severity: str
    status: str
    detail: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "check_id": self.check_id,
            "name": self.name,
            "target": self.target,
            "purpose": self.purpose,
            "criteria": self.criteria,
            "severity": self.severity,
            "status": self.status,
            "detail": self.detail,
        }


@dataclass
class SourceState:
    name: str
    records_read: int = 0
    records_accepted: int = 0
    records_output: int = 0
    records_rejected: int = 0
    records_deduplicated: int = 0
    records_excluded: int = 0
    records_modified: int = 0
    rules_applied: dict[str, int] = field(default_factory=dict)
    rejected_frames: list[pl.DataFrame] = field(default_factory=list)
    parent_run_id: str | None = None
    source_run_ids: list[str] = field(default_factory=list)

    def add_rule(self, rule_id: str, count: int) -> None:
        if count:
            self.rules_applied[rule_id] = self.rules_applied.get(rule_id, 0) + count


@dataclass
class TransformationResult:
    table_name: str
    run_id: str
    decision: str
    status: str
    checks: list[CheckResult] = field(default_factory=list)
    records_read: int = 0
    records_output: int = 0
    records_rejected: int = 0
    records_deduplicated: int = 0
    records_excluded: int = 0
    records_modified: int = 0
    rules_applied: dict[str, int] = field(default_factory=dict)
    output_path: str | None = None
    quarantine_path: str | None = None
    message: str = ""

    @property
    def check_map(self) -> dict[str, str]:
        return {c.check_id: c.status for c in self.checks}

    @property
    def is_success(self) -> bool:
        return self.decision in (APPROVED, APPROVED_WITH_WARNING)
