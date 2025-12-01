"""Built-in constraint profiles."""

from __future__ import annotations

from .profile import ConstraintProfile, RelationRule, RelationRuleType


flow_safe = ConstraintProfile(
    name="flow_safe",
    description="Conservative profile for telemetry with minimal repairs.",
    numeric_policy={"clip": True, "impute": "median", "nan_policy": "impute"},
    int_policy={"rounding": "nearest", "strict": True},
    relation_rules=[
        RelationRule(
            name="packet_order",
            type=RelationRuleType.ORDER,
            params={"left": "bytes", "right": "packets", "operator": ">="},
            message="Bytes expected to be at least packets",
            severity="warn",
            repair_strategy="minimize_delta",
        )
    ],
    severity_map={"range": "warn", "type": "error", "null": "error"},
    repair_mode="safe",
)

strict_flow = ConstraintProfile(
    name="strict_flow",
    description="Strict bounds and rejection of missing values.",
    numeric_policy={"clip": True, "impute": "none", "nan_policy": "reject"},
    int_policy={"rounding": "nearest", "strict": True},
    relation_rules=[
        RelationRule(
            name="percentile_order",
            type=RelationRuleType.NONDECREASING_GROUP,
            params={"columns": ["p25", "p50", "p75", "p90", "p95", "p99"]},
            message="Percentiles must be non-decreasing",
            severity="error",
            repair_strategy="minimize_delta",
        )
    ],
    severity_map={"range": "error", "type": "error", "null": "error"},
    repair_mode="safe",
)

telemetry_noisy = ConstraintProfile(
    name="telemetry_noisy",
    description="Tolerant profile for noisy telemetry, repairs aggressively.",
    numeric_policy={"clip": True, "impute": "mean", "nan_policy": "impute"},
    int_policy={"rounding": "nearest", "strict": False},
    relation_rules=[
        RelationRule(
            name="duration_non_negative",
            type=RelationRuleType.IF_THEN,
            params={"if_feature": "duration", "op": ">=", "value": 0, "then_feature": "bytes", "constraint": {"min": 0}},
            message="Duration implies non-negative bytes",
            severity="warn",
            repair_strategy="clip",
        )
    ],
    severity_map={"range": "warn", "type": "warn", "null": "warn"},
    repair_mode="aggressive",
)

__all__ = ["flow_safe", "strict_flow", "telemetry_noisy"]
