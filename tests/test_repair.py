import pandas as pd

from flowshield import FlowShield, FeatureSchema, profiles


def test_repair_clips_negative():
    schema = FeatureSchema.from_columns(["duration"], defaults={"dtype": "float", "minimum": 0, "non_negative": True})
    df = pd.DataFrame({"duration": [-5.0]})
    flow = FlowShield()
    flow.fit_repair_stats(df, schema)
    repaired, report = flow.repair(df, schema, profiles.flow_safe)
    assert repaired.iloc[0]["duration"] >= 0
    assert report.total_actions >= 1
