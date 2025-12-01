"""Demo for FlowShield."""

from __future__ import annotations

import pandas as pd

from flowshield import FlowShield, FeatureSchema, profiles


def main() -> None:
    schema = FeatureSchema.from_json("example_schema.json")
    df = pd.DataFrame(
        {
            "duration": [1.2, -1.0],
            "packets": [10.4, 3],
            "bytes": [12, 1],
            "p25": [10, 5],
            "p50": [9, 6],
            "p75": [12, 8],
            "p95": [15, 9],
            "p99": [20, 10],
        }
    )
    flow = FlowShield()
    flow.fit_repair_stats(df, schema)
    repaired, report = flow.repair(df, schema, profiles.flow_safe)
    print("Repaired dataframe:\n", repaired)
    print("Validation after repair:", report.validation_after.total_violations)


if __name__ == "__main__":
    main()
