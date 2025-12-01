import pandas as pd

from flowshield import FlowShield, FeatureSchema, profiles


def test_validate_detects_violation():
    schema = FeatureSchema.from_columns(["duration", "packets"], defaults={"dtype": "float", "minimum": 0})
    df = pd.DataFrame({"duration": [-1], "packets": [1]})
    flow = FlowShield()
    report = flow.validate(df, schema, profiles.flow_safe)
    assert report.total_violations >= 1
