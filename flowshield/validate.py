"""Validation engine."""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from .profile import ConstraintProfile
from .rules import ConstraintViolation, evaluate_relation
from .schema import FeatureSchema


def _infer_severity(profile: ConstraintProfile, violation_type: str) -> str:
    return profile.severity_map.get(violation_type, "warn")


def _validate_value(
    row_index: int,
    col_name: str,
    value: object,
    col_spec,
    profile: ConstraintProfile,
) -> List[ConstraintViolation]:
    violations: List[ConstraintViolation] = []
    severity = _infer_severity(profile, "type")
    if value is None or (isinstance(value, float) and np.isnan(value)):
        if not col_spec.nullable and profile.numeric_policy.get("nan_policy", "reject") == "reject":
            violations.append(
                ConstraintViolation(
                    row_index=row_index,
                    column=col_name,
                    rule_name="null_check",
                    violation_type="null",
                    severity=profile.severity_map.get("null", "error"),
                    observed_value=value,
                    expected="non-null",
                    message=f"Column {col_name} does not allow nulls",
                )
            )
        return violations

    dtype = col_spec.dtype
    if dtype in {"float", "int"}:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            violations.append(
                ConstraintViolation(
                    row_index=row_index,
                    column=col_name,
                    rule_name="type_check",
                    violation_type="type",
                    severity=severity,
                    observed_value=value,
                    expected=dtype,
                    message=f"Value for {col_name} is not numeric",
                )
            )
            return violations
        if dtype == "int" or col_spec.integer_only:
            if abs(numeric - round(numeric)) > 1e-6:
                violations.append(
                    ConstraintViolation(
                        row_index=row_index,
                        column=col_name,
                        rule_name="integer_check",
                        violation_type="integer",
                        severity=_infer_severity(profile, "integer"),
                        observed_value=value,
                        expected="integer",
                        message=f"Column {col_name} requires integer values",
                    )
                )
        if col_spec.minimum is not None and numeric < col_spec.minimum:
            violations.append(
                ConstraintViolation(
                    row_index=row_index,
                    column=col_name,
                    rule_name="min_check",
                    violation_type="range",
                    severity=_infer_severity(profile, "range"),
                    observed_value=value,
                    expected=f">={col_spec.minimum}",
                    message=f"{col_name} below minimum",
                )
            )
        if col_spec.maximum is not None and numeric > col_spec.maximum:
            violations.append(
                ConstraintViolation(
                    row_index=row_index,
                    column=col_name,
                    rule_name="max_check",
                    violation_type="range",
                    severity=_infer_severity(profile, "range"),
                    observed_value=value,
                    expected=f"<={col_spec.maximum}",
                    message=f"{col_name} above maximum",
                )
            )
        if col_spec.non_negative and numeric < 0:
            violations.append(
                ConstraintViolation(
                    row_index=row_index,
                    column=col_name,
                    rule_name="non_negative",
                    violation_type="range",
                    severity=_infer_severity(profile, "range"),
                    observed_value=value,
                    expected=">=0",
                    message=f"{col_name} must be non-negative",
                )
            )
    elif dtype == "bool":
        if value not in {0, 1, True, False}:
            violations.append(
                ConstraintViolation(
                    row_index=row_index,
                    column=col_name,
                    rule_name="bool_check",
                    violation_type="type",
                    severity=_infer_severity(profile, "type"),
                    observed_value=value,
                    expected="0/1 or True/False",
                    message=f"{col_name} must be boolean",
                )
            )
    elif dtype == "categorical":
        if col_spec.categories and value not in col_spec.categories:
            violations.append(
                ConstraintViolation(
                    row_index=row_index,
                    column=col_name,
                    rule_name="category_check",
                    violation_type="category",
                    severity=_infer_severity(profile, "category"),
                    observed_value=value,
                    expected=",".join(col_spec.categories),
                    message=f"Unexpected category for {col_name}",
                )
            )
        elif not isinstance(value, str):
            violations.append(
                ConstraintViolation(
                    row_index=row_index,
                    column=col_name,
                    rule_name="category_check",
                    violation_type="category",
                    severity=_infer_severity(profile, "category"),
                    observed_value=value,
                    expected="string",
                    message=f"Categorical value for {col_name} must be a string",
                )
            )
    return violations


def validate_dataframe(
    df: pd.DataFrame,
    schema: FeatureSchema,
    profile: ConstraintProfile,
    sample_limit: int | None = None,
) -> Tuple[List[ConstraintViolation], int]:
    """Validate dataframe against schema and profile. Returns violations and row count considered."""

    if sample_limit:
        df = df.head(sample_limit)

    violations: List[ConstraintViolation] = []
    for idx, row in df.iterrows():
        for col in schema.columns:
            if col.name not in row:
                violations.append(
                    ConstraintViolation(
                        row_index=int(idx),
                        column=col.name,
                        rule_name="missing_column",
                        violation_type="missing",
                        severity="error",
                        observed_value=None,
                        expected="column present",
                        message=f"Column {col.name} missing in data",
                    )
                )
                continue
            value = row[col.name]
            violations.extend(_validate_value(int(idx), col.name, value, col, profile))
        for rule in profile.relation_rules:
            ok, violation, _ = evaluate_relation(row, rule)
            if not ok and violation:
                violations.append(violation)

    return violations, len(df)


def build_validation_report(
    violations: List[ConstraintViolation], total_rows: int
) -> "ValidationReport":
    from .report import ValidationReport

    total_violations = len(violations)
    by_severity = Counter(v.severity for v in violations)
    by_column: Dict[str, int] = defaultdict(int)
    by_rule: Dict[str, int] = defaultdict(int)
    for v in violations:
        if v.column:
            by_column[v.column] += 1
        by_rule[v.rule_name] += 1

    top_columns = sorted(by_column.items(), key=lambda x: x[1], reverse=True)[:5]
    top_rules = sorted(by_rule.items(), key=lambda x: x[1], reverse=True)[:5]

    return ValidationReport(
        total_rows=total_rows,
        total_violations=total_violations,
        violations_by_severity=dict(by_severity),
        top_violated_columns=top_columns,
        top_violated_rules=top_rules,
        sample_violations=violations[:50],
    )
