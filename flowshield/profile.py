"""Profiles and relation rules."""

from __future__ import annotations

from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, model_validator

from .exceptions import ProfileError


class RelationRuleType(str, Enum):
    """Supported relation rule types."""

    SUM_BOUNDS = "SUM_BOUNDS"
    ORDER = "ORDER"
    RATIO_BOUNDS = "RATIO_BOUNDS"
    IF_THEN = "IF_THEN"
    NONDECREASING_GROUP = "NONDECREASING_GROUP"


class RelationRule(BaseModel):
    """A rule describing relationships between columns."""

    name: str
    type: RelationRuleType
    params: Dict[str, object]
    message: str
    severity: str = Field("warn", pattern="^(info|warn|error)$")
    repair_strategy: str = Field("none", pattern="^(none|clip|minimize_delta)$")

    @model_validator(mode="after")
    def validate_rule(self) -> "RelationRule":
        if not self.name:
            raise ProfileError("Rule name must be provided")
        return self


class ConstraintProfile(BaseModel):
    """Constraint profile defining policies and relation rules."""

    name: str
    description: str
    numeric_policy: Dict[str, object]
    int_policy: Dict[str, object]
    relation_rules: List[RelationRule] = Field(default_factory=list)
    severity_map: Dict[str, str] = Field(default_factory=dict)
    repair_mode: str = Field("safe", pattern="^(safe|aggressive)$")
    deterministic_seed: int = 1337

    @model_validator(mode="after")
    def validate_profile(self) -> "ConstraintProfile":
        if self.numeric_policy.get("impute") not in {"none", "median", "mean", "zero"}:
            raise ProfileError("numeric_policy.impute must be one of none|median|mean|zero")
        if self.numeric_policy.get("nan_policy") not in {"reject", "impute"}:
            raise ProfileError("numeric_policy.nan_policy must be reject|impute")
        if self.int_policy.get("rounding") not in {"nearest", "floor", "ceil"}:
            raise ProfileError("int_policy.rounding must be nearest|floor|ceil")
        if self.repair_mode not in {"safe", "aggressive"}:
            raise ProfileError("repair_mode must be safe|aggressive")
        return self
