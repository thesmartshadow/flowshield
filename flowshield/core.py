"""FlowShield orchestrator."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Tuple

import pandas as pd

from .profile import ConstraintProfile
from .repair import RepairContext, generate_repair_report, repair_dataframe
from .report import RepairReport, ValidationReport
from .schema import FeatureSchema
from .validate import build_validation_report, validate_dataframe


class FlowShield:
    """Main entry point for validation and repair."""

    def __init__(self) -> None:
        self.context = RepairContext()

    def fit_repair_stats(self, df_train: pd.DataFrame, schema: FeatureSchema) -> None:
        """Fit imputation statistics from training data."""
        self.context.update_stats(df_train, schema)

    def validate(
        self,
        df: pd.DataFrame,
        schema: FeatureSchema,
        profile: ConstraintProfile,
        *,
        sample_limit: int | None = None,
    ) -> ValidationReport:
        violations, total_rows = validate_dataframe(df, schema, profile, sample_limit)
        return build_validation_report(violations, total_rows)

    def repair(
        self,
        df: pd.DataFrame,
        schema: FeatureSchema,
        profile: ConstraintProfile,
        *,
        keep_actions: bool = True,
    ) -> Tuple[pd.DataFrame, RepairReport]:
        validation_before = self.validate(df, schema, profile)
        repaired_df, actions = repair_dataframe(df, schema, profile, self.context, keep_actions)
        validation_after = self.validate(repaired_df, schema, profile)
        report = generate_repair_report(actions, validation_before, validation_after)
        return repaired_df, report

    def save_state(self, path: str | Path) -> None:
        state = {"impute_stats": self.context.impute_stats}
        Path(path).write_text(json.dumps(state))

    @classmethod
    def load_state(cls, path: str | Path) -> "FlowShield":
        content = Path(path).read_text()
        data = json.loads(content)
        obj = cls()
        obj.context.impute_stats = data.get("impute_stats", {})
        return obj
