"""Reporting utilities."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from .rules import ConstraintViolation, RepairAction


@dataclass
class ValidationReport:
    """Summary of constraint violations."""

    total_rows: int
    total_violations: int
    violations_by_severity: Dict[str, int]
    top_violated_columns: List[Tuple[str, int]]
    top_violated_rules: List[Tuple[str, int]]
    sample_violations: List[ConstraintViolation]

    def to_json(self, path: str | Path) -> None:
        Path(path).write_text(self._as_json())

    def to_markdown(self, path: str | Path) -> None:
        Path(path).write_text(self._as_markdown())

    def _as_json(self) -> str:
        def serialize_violation(v: ConstraintViolation) -> Dict[str, object]:
            return asdict(v)

        payload = asdict(self)
        payload["sample_violations"] = [serialize_violation(v) for v in self.sample_violations]
        import json

        return json.dumps(payload, indent=2)

    def _as_markdown(self) -> str:
        lines = ["# FlowShield Validation Report", ""]
        lines.append(f"Total rows: {self.total_rows}")
        lines.append(f"Total violations: {self.total_violations}")
        lines.append("\n## Violations by severity")
        for sev, count in self.violations_by_severity.items():
            lines.append(f"- {sev}: {count}")
        lines.append("\n## Top violated columns")
        for col, count in self.top_violated_columns:
            lines.append(f"- {col}: {count}")
        lines.append("\n## Top violated rules")
        for rule, count in self.top_violated_rules:
            lines.append(f"- {rule}: {count}")
        lines.append("\n## Sample violations")
        for v in self.sample_violations[:50]:
            lines.append(
                f"- Row {v.row_index} | {v.rule_name} | {v.column or 'rule'} | {v.violation_type}: {v.message} (observed={v.observed_value}, expected={v.expected})"
            )
        lines.append("\n## How to fix")
        lines.append("Focus on the highest severity violations first. Check schema bounds and relation rules for the most frequent columns and rules listed above.")
        return "\n".join(lines)


@dataclass
class RepairReport:
    """Summary of repair actions and validation state."""

    total_actions: int
    actions_by_column: Dict[str, int]
    actions_by_rule: Dict[str, int]
    max_delta_per_column: Dict[str, float]
    actions: Optional[List[RepairAction]]
    validation_before: ValidationReport
    validation_after: ValidationReport

    def to_json(self, path: str | Path) -> None:
        Path(path).write_text(self._as_json())

    def to_markdown(self, path: str | Path) -> None:
        Path(path).write_text(self._as_markdown())

    def _as_json(self) -> str:
        import json

        def serialize_action(a: RepairAction) -> Dict[str, object]:
            return asdict(a)

        payload = {
            "total_actions": self.total_actions,
            "actions_by_column": self.actions_by_column,
            "actions_by_rule": self.actions_by_rule,
            "max_delta_per_column": self.max_delta_per_column,
            "actions": [serialize_action(a) for a in self.actions] if self.actions else None,
            "validation_before": asdict(self.validation_before),
            "validation_after": asdict(self.validation_after),
        }
        payload["validation_before"]["sample_violations"] = [asdict(v) for v in self.validation_before.sample_violations]
        payload["validation_after"]["sample_violations"] = [asdict(v) for v in self.validation_after.sample_violations]
        return json.dumps(payload, indent=2)

    def _as_markdown(self) -> str:
        lines = ["# FlowShield Repair Report", ""]
        lines.append(f"Total actions: {self.total_actions}")
        lines.append("\n## Actions by column")
        for col, count in self.actions_by_column.items():
            lines.append(f"- {col}: {count}")
        lines.append("\n## Actions by rule")
        for rule, count in self.actions_by_rule.items():
            lines.append(f"- {rule}: {count}")
        lines.append("\n## Maximum delta per column")
        for col, delta in self.max_delta_per_column.items():
            lines.append(f"- {col}: {delta}")
        lines.append("\n## Validation before repair")
        lines.append(self.validation_before._as_markdown())
        lines.append("\n## Validation after repair")
        lines.append(self.validation_after._as_markdown())
        return "\n".join(lines)
