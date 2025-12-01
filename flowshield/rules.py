"""Relation rules and violation/repair data models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from .profile import ConstraintProfile, RelationRule, RelationRuleType


@dataclass
class ConstraintViolation:
    """A single constraint violation."""

    row_index: int
    column: Optional[str]
    rule_name: str
    violation_type: str
    severity: str
    observed_value: Any
    expected: str
    message: str


@dataclass
class RepairAction:
    """A single repair action applied to a cell."""

    row_index: int
    column: str
    old_value: Any
    new_value: Any
    reason: str
    rule_name: str
    strategy: str
    delta: Optional[float] = None


def _safe_float(value: Any) -> float:
    try:
        if pd.isna(value):
            return float("nan")
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def evaluate_relation(
    row: pd.Series, rule: RelationRule
) -> Tuple[bool, Optional[ConstraintViolation], Dict[str, float]]:
    """Evaluate a relation rule on a row, returning violation info and numeric values used."""

    values: Dict[str, float] = {}
    for key, val in rule.params.items():
        if isinstance(val, str) and val in row.index:
            values[val] = _safe_float(row[val])
    violation: Optional[ConstraintViolation] = None
    ok = True

    if rule.type == RelationRuleType.SUM_BOUNDS:
        columns: Sequence[str] = rule.params.get("columns", [])  # type: ignore[assignment]
        total = float(np.nansum([_safe_float(row[c]) for c in columns]))
        minimum = float(rule.params.get("min", float("-inf")))
        maximum = float(rule.params.get("max", float("inf")))
        if total < minimum or total > maximum:
            ok = False
            violation = ConstraintViolation(
                row_index=int(row.name),
                column=None,
                rule_name=rule.name,
                violation_type="sum_bounds",
                severity=rule.severity,
                observed_value=total,
                expected=f"{minimum} <= sum({','.join(columns)}) <= {maximum}",
                message=rule.message,
            )
    elif rule.type == RelationRuleType.ORDER:
        left = str(rule.params.get("left"))
        right = str(rule.params.get("right"))
        operator = str(rule.params.get("operator", ">="))
        lv = _safe_float(row[left])
        rv = _safe_float(row[right])
        if operator == ">=" and not (lv >= rv):
            ok = False
        elif operator == "<=" and not (lv <= rv):
            ok = False
        if not ok:
            violation = ConstraintViolation(
                row_index=int(row.name),
                column=None,
                rule_name=rule.name,
                violation_type="order",
                severity=rule.severity,
                observed_value={left: lv, right: rv},
                expected=f"{left} {operator} {right}",
                message=rule.message,
            )
    elif rule.type == RelationRuleType.RATIO_BOUNDS:
        num = str(rule.params.get("numerator"))
        den = str(rule.params.get("denominator"))
        eps = float(rule.params.get("eps", 1e-6))
        rv = _safe_float(row[den])
        ratio = _safe_float(row[num]) / (rv + eps)
        minimum = float(rule.params.get("min", float("-inf")))
        maximum = float(rule.params.get("max", float("inf")))
        if ratio < minimum or ratio > maximum:
            ok = False
            violation = ConstraintViolation(
                row_index=int(row.name),
                column=None,
                rule_name=rule.name,
                violation_type="ratio_bounds",
                severity=rule.severity,
                observed_value=ratio,
                expected=f"{minimum} <= {num}/{den} <= {maximum}",
                message=rule.message,
            )
    elif rule.type == RelationRuleType.IF_THEN:
        feature = str(rule.params.get("if_feature"))
        op = str(rule.params.get("op", ">="))
        value = float(rule.params.get("value", 0))
        then_feature = str(rule.params.get("then_feature"))
        constraint = rule.params.get("constraint", "non_negative")
        fv = _safe_float(row[feature])
        condition = (op == ">=" and fv >= value) or (op == "<=" and fv <= value)
        if condition:
            target = _safe_float(row[then_feature])
            violated = False
            expected = ""
            if constraint == "non_negative" and target < 0:
                violated = True
                expected = f"{then_feature} >= 0 when {feature} {op} {value}"
            elif isinstance(constraint, dict) and "min" in constraint:
                if target < float(constraint["min"]):
                    violated = True
                    expected = f"{then_feature} >= {constraint['min']} when condition met"
            elif isinstance(constraint, dict) and "max" in constraint:
                if target > float(constraint["max"]):
                    violated = True
                    expected = f"{then_feature} <= {constraint['max']} when condition met"
            if violated:
                ok = False
                violation = ConstraintViolation(
                    row_index=int(row.name),
                    column=then_feature,
                    rule_name=rule.name,
                    violation_type="if_then",
                    severity=rule.severity,
                    observed_value=row[then_feature],
                    expected=expected,
                    message=rule.message,
                )
    elif rule.type == RelationRuleType.NONDECREASING_GROUP:
        columns: Sequence[str] = rule.params.get("columns", [])  # type: ignore[assignment]
        prev = -float("inf")
        for col in columns:
            val = _safe_float(row[col])
            if val < prev - 1e-9:
                ok = False
                violation = ConstraintViolation(
                    row_index=int(row.name),
                    column=col,
                    rule_name=rule.name,
                    violation_type="nondecreasing",
                    severity=rule.severity,
                    observed_value=row[col],
                    expected=f"{columns}",
                    message=rule.message,
                )
                break
            prev = val
    return ok, violation, values


def repair_relation(
    row: pd.Series,
    rule: RelationRule,
    profile: ConstraintProfile,
    bounds: Dict[str, Dict[str, float]],
) -> Tuple[pd.Series, List[RepairAction]]:
    """Repair row to satisfy relation rule if possible."""

    actions: List[RepairAction] = []
    ok, violation, _ = evaluate_relation(row, rule)
    if ok or rule.repair_strategy == "none":
        return row, actions

    if rule.type == RelationRuleType.ORDER:
        left = str(rule.params.get("left"))
        right = str(rule.params.get("right"))
        operator = str(rule.params.get("operator", ">="))
        lv = _safe_float(row[left])
        rv = _safe_float(row[right])
        if operator == ">=":
            if profile.repair_mode == "safe":
                target = max(lv, rv)
                if np.isfinite(bounds.get(left, {}).get("max", np.inf)):
                    new_lv = max(rv, bounds[left]["min"] if "min" in bounds.get(left, {}) else rv)
                    new_lv = min(new_lv, bounds[left].get("max", new_lv))
                else:
                    new_lv = target
                if new_lv != row[left]:
                    actions.append(
                        RepairAction(
                            row_index=int(row.name),
                            column=left,
                            old_value=row[left],
                            new_value=new_lv,
                            reason=violation.message if violation else "order repair",
                            rule_name=rule.name,
                            strategy=rule.repair_strategy,
                            delta=float(new_lv - lv),
                        )
                    )
                    row[left] = new_lv
            else:
                midpoint = (lv + rv) / 2
                new_lv = max(midpoint, bounds.get(left, {}).get("min", -np.inf))
                new_rv = min(midpoint, bounds.get(right, {}).get("max", np.inf))
                if new_lv != row[left]:
                    actions.append(
                        RepairAction(
                            row_index=int(row.name),
                            column=left,
                            old_value=row[left],
                            new_value=new_lv,
                            reason=violation.message if violation else "order repair",
                            rule_name=rule.name,
                            strategy=rule.repair_strategy,
                            delta=float(new_lv - lv),
                        )
                    )
                    row[left] = new_lv
                if new_rv != row[right]:
                    actions.append(
                        RepairAction(
                            row_index=int(row.name),
                            column=right,
                            old_value=row[right],
                            new_value=new_rv,
                            reason=violation.message if violation else "order repair",
                            rule_name=rule.name,
                            strategy=rule.repair_strategy,
                            delta=float(new_rv - rv),
                        )
                    )
                    row[right] = new_rv
        elif operator == "<=":
            rule.params = {"left": right, "right": left, "operator": ">="}
            return repair_relation(row, rule, profile, bounds)
    elif rule.type == RelationRuleType.SUM_BOUNDS:
        columns: Sequence[str] = rule.params.get("columns", [])  # type: ignore[assignment]
        minimum = float(rule.params.get("min", float("-inf")))
        maximum = float(rule.params.get("max", float("inf")))
        current = float(np.nansum([_safe_float(row[c]) for c in columns]))
        if current < minimum:
            deficit = minimum - current
            share = deficit / len(columns)
            for col in columns:
                old = _safe_float(row[col])
                new_val = old + share
                max_bound = bounds.get(col, {}).get("max", np.inf)
                new_val = min(new_val, max_bound)
                actions.append(
                    RepairAction(
                        row_index=int(row.name),
                        column=col,
                        old_value=row[col],
                        new_value=new_val,
                        reason="sum_bounds repair",
                        rule_name=rule.name,
                        strategy=rule.repair_strategy,
                        delta=float(new_val - old),
                    )
                )
                row[col] = new_val
        elif current > maximum:
            surplus = current - maximum
            share = surplus / len(columns)
            for col in columns:
                old = _safe_float(row[col])
                new_val = old - share
                min_bound = bounds.get(col, {}).get("min", -np.inf)
                new_val = max(new_val, min_bound)
                actions.append(
                    RepairAction(
                        row_index=int(row.name),
                        column=col,
                        old_value=row[col],
                        new_value=new_val,
                        reason="sum_bounds repair",
                        rule_name=rule.name,
                        strategy=rule.repair_strategy,
                        delta=float(new_val - old),
                    )
                )
                row[col] = new_val
    elif rule.type == RelationRuleType.RATIO_BOUNDS:
        num = str(rule.params.get("numerator"))
        den = str(rule.params.get("denominator"))
        eps = float(rule.params.get("eps", 1e-6))
        minimum = float(rule.params.get("min", float("-inf")))
        maximum = float(rule.params.get("max", float("inf")))
        rv = _safe_float(row[den])
        ratio = _safe_float(row[num]) / (rv + eps)
        if ratio < minimum:
            desired_num = minimum * (rv + eps)
            old = row[num]
            row[num] = desired_num
            actions.append(
                RepairAction(
                    row_index=int(row.name),
                    column=num,
                    old_value=old,
                    new_value=desired_num,
                    reason="ratio lower bound",
                    rule_name=rule.name,
                    strategy=rule.repair_strategy,
                    delta=float(desired_num - _safe_float(old)),
                )
            )
        elif ratio > maximum:
            desired_num = maximum * (rv + eps)
            old = row[num]
            row[num] = desired_num
            actions.append(
                RepairAction(
                    row_index=int(row.name),
                    column=num,
                    old_value=old,
                    new_value=desired_num,
                    reason="ratio upper bound",
                    rule_name=rule.name,
                    strategy=rule.repair_strategy,
                    delta=float(desired_num - _safe_float(old)),
                )
            )
    elif rule.type == RelationRuleType.NONDECREASING_GROUP:
        columns: Sequence[str] = rule.params.get("columns", [])  # type: ignore[assignment]
        values = [_safe_float(row[c]) for c in columns]
        corrected = []
        current_max = -np.inf
        for val in values:
            if val < current_max:
                corrected.append(current_max)
            else:
                corrected.append(val)
                current_max = val
        for col, old, new_val in zip(columns, values, corrected):
            if new_val != old:
                actions.append(
                    RepairAction(
                        row_index=int(row.name),
                        column=col,
                        old_value=row[col],
                        new_value=new_val,
                        reason="nondecreasing repair",
                        rule_name=rule.name,
                        strategy=rule.repair_strategy,
                        delta=float(new_val - old),
                    )
                )
                row[col] = new_val
    return row, actions


def bounds_from_schema(schema_columns: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    """Utility for extracting bounds from schema column metadata."""

    bounds: Dict[str, Dict[str, float]] = {}
    for name, meta in schema_columns.items():
        column_bounds: Dict[str, float] = {}
        if meta.get("minimum") is not None:
            column_bounds["min"] = float(meta["minimum"])
        if meta.get("maximum") is not None:
            column_bounds["max"] = float(meta["maximum"])
        if meta.get("non_negative"):
            column_bounds["min"] = max(0.0, column_bounds.get("min", 0.0))
        bounds[name] = column_bounds
    return bounds
