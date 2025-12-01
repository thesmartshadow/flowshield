import pandas as pd

from flowshield.profile import RelationRule, RelationRuleType
from flowshield.rules import evaluate_relation


def test_order_rule_violation():
    rule = RelationRule(
        name="order",
        type=RelationRuleType.ORDER,
        params={"left": "a", "right": "b", "operator": ">="},
        message="a should be >= b",
    )
    row = pd.Series({"a": 1, "b": 5})
    ok, violation, _ = evaluate_relation(row, rule)
    assert not ok
    assert violation is not None
