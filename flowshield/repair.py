"""Repair engine for FlowShield."""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from .profile import ConstraintProfile
from .rules import ConstraintViolation, RepairAction, bounds_from_schema, repair_relation
from .schema import FeatureSchema
from .validate import build_validation_report, validate_dataframe


class RepairContext:
    """Context holding statistics for repairs."""

    def __init__(self) -> None:
        self.impute_stats: Dict[str, float] = {}

    def update_stats(self, df: pd.DataFrame, schema: FeatureSchema) -> None:
        for col in schema.columns:
            if col.dtype in {"float", "int"}:
                series = pd.to_numeric(df[col.name], errors="coerce")
                self.impute_stats[col.name] = float(series.median())

    def get_impute_value(self, col_name: str, strategy: str) -> float:
        if strategy == "zero":
            return 0.0
        if strategy == "mean":
            return float(self.impute_stats.get(col_name, 0.0))
        if strategy == "median":
            return float(self.impute_stats.get(col_name, 0.0))
        return 0.0


def _round_value(value: float, strategy: str) -> float:
    if strategy == "floor":
        return float(np.floor(value))
    if strategy == "ceil":
        return float(np.ceil(value))
    return float(np.round(value))


def _coerce_numeric(value: object) -> float:
    try:
        if value is None:
            return float("nan")
        if isinstance(value, bool):
            return float(value)
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def repair_dataframe(
    df: pd.DataFrame,
    schema: FeatureSchema,
    profile: ConstraintProfile,
    context: RepairContext,
    keep_actions: bool = True,
) -> Tuple[pd.DataFrame, List[RepairAction]]:
    repaired = df.copy(deep=True)
    actions: List[RepairAction] = []
    bounds = bounds_from_schema({c.name: c.model_dump() for c in schema.columns})

    for idx, row in repaired.iterrows():
        for col in schema.columns:
            value = row[col.name]
            original_value = value
            if col.dtype in {"float", "int"}:
                numeric = _coerce_numeric(value)
                if np.isnan(numeric):
                    if profile.numeric_policy.get("nan_policy") == "impute":
                        numeric = context.get_impute_value(col.name, str(profile.numeric_policy.get("impute")))
                        repaired.at[idx, col.name] = numeric
                        actions.append(
                            RepairAction(
                                row_index=int(idx),
                                column=col.name,
                                old_value=original_value,
                                new_value=numeric,
                                reason="impute",
                                rule_name="nan_policy",
                                strategy=str(profile.numeric_policy.get("impute")),
                                delta=None,
                            )
                        )
                    else:
                        continue
                if col.non_negative and numeric < 0:
                    new_val = max(0.0, numeric)
                    repaired.at[idx, col.name] = new_val
                    actions.append(
                        RepairAction(
                            row_index=int(idx),
                            column=col.name,
                            old_value=original_value,
                            new_value=new_val,
                            reason="non_negative",
                            rule_name="non_negative",
                            strategy="clip",
                            delta=float(new_val - _coerce_numeric(original_value)),
                        )
                    )
                    numeric = new_val
                if col.minimum is not None and numeric < col.minimum and profile.numeric_policy.get("clip", False):
                    new_val = col.minimum
                    repaired.at[idx, col.name] = new_val
                    actions.append(
                        RepairAction(
                            row_index=int(idx),
                            column=col.name,
                            old_value=original_value,
                            new_value=new_val,
                            reason="min_clip",
                            rule_name="min_check",
                            strategy="clip",
                            delta=float(new_val - _coerce_numeric(original_value)),
                        )
                    )
                    numeric = new_val
                if col.maximum is not None and numeric > col.maximum and profile.numeric_policy.get("clip", False):
                    new_val = col.maximum
                    repaired.at[idx, col.name] = new_val
                    actions.append(
                        RepairAction(
                            row_index=int(idx),
                            column=col.name,
                            old_value=original_value,
                            new_value=new_val,
                            reason="max_clip",
                            rule_name="max_check",
                            strategy="clip",
                            delta=float(new_val - _coerce_numeric(original_value)),
                        )
                    )
                    numeric = new_val
                if col.integer_only or col.dtype == "int":
                    rounded = _round_value(numeric, str(profile.int_policy.get("rounding", "nearest")))
                    if rounded != numeric:
                        repaired.at[idx, col.name] = rounded
                        actions.append(
                            RepairAction(
                                row_index=int(idx),
                                column=col.name,
                                old_value=original_value,
                                new_value=rounded,
                                reason="integer_only",
                                rule_name="integer_check",
                                strategy=str(profile.int_policy.get("rounding")),
                                delta=float(rounded - _coerce_numeric(original_value)),
                            )
                        )
            elif col.dtype == "bool":
                if value in {True, False, 0, 1}:
                    repaired.at[idx, col.name] = bool(value)
                else:
                    if profile.repair_mode == "aggressive":
                        new_val = bool(value)
                        repaired.at[idx, col.name] = new_val
                        actions.append(
                            RepairAction(
                                row_index=int(idx),
                                column=col.name,
                                old_value=original_value,
                                new_value=new_val,
                                reason="coerce bool",
                                rule_name="bool_check",
                                strategy="coerce",
                                delta=None,
                            )
                        )
            elif col.dtype == "categorical":
                if col.categories and value not in col.categories:
                    if profile.repair_mode == "aggressive":
                        repaired.at[idx, col.name] = None
                        actions.append(
                            RepairAction(
                                row_index=int(idx),
                                column=col.name,
                                old_value=original_value,
                                new_value=None,
                                reason="unknown category",
                                rule_name="category_check",
                                strategy="nullify",
                                delta=None,
                            )
                        )
            # update row for relation rules
            row[col.name] = repaired.at[idx, col.name]
        for rule in profile.relation_rules:
            row, relation_actions = repair_relation(row, rule, profile, bounds)
            for act in relation_actions:
                repaired.at[idx, act.column] = act.new_value
            actions.extend(relation_actions)
    return repaired, actions if keep_actions else []


def generate_repair_report(
    actions: List[RepairAction], validation_before, validation_after
) -> "RepairReport":
    from .report import RepairReport

    actions_by_column: Dict[str, int] = Counter(act.column for act in actions)
    actions_by_rule: Dict[str, int] = Counter(act.rule_name for act in actions)
    max_delta: Dict[str, float] = defaultdict(float)
    for act in actions:
        if act.delta is not None:
            max_delta[act.column] = max(max_delta[act.column], abs(act.delta))
    return RepairReport(
        total_actions=len(actions),
        actions_by_column=dict(actions_by_column),
        actions_by_rule=dict(actions_by_rule),
        max_delta_per_column=dict(max_delta),
        actions=actions,
        validation_before=validation_before,  # type: ignore[arg-type]
        validation_after=validation_after,  # type: ignore[arg-type]
    )
