import json
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from flowshield.cli import app


def test_cli_profiles_lists():
    runner = CliRunner()
    result = runner.invoke(app, ["profiles"])
    assert result.exit_code == 0
    assert "flow_safe" in result.stdout


def test_cli_validate_and_repair(tmp_path: Path):
    data_path = tmp_path / "data.csv"
    df = pd.DataFrame({"duration": [1.0], "packets": [1]})
    df.to_csv(data_path, index=False)
    schema_path = tmp_path / "schema.json"
    from flowshield.schema import FeatureSchema

    FeatureSchema.from_columns(["duration", "packets"], defaults={"dtype": "float", "minimum": 0}).to_json(schema_path)

    runner = CliRunner()
    report_path = tmp_path / "report.json"
    result = runner.invoke(
        app,
        ["validate", "--data", str(data_path), "--schema", str(schema_path), "--profile", "flow_safe", "--out", str(report_path)],
    )
    assert result.exit_code == 0
    assert report_path.exists()

    repaired_path = tmp_path / "repaired.csv"
    repair_report_path = tmp_path / "repair_report.json"
    result = runner.invoke(
        app,
        [
            "repair",
            "--data",
            str(data_path),
            "--schema",
            str(schema_path),
            "--profile",
            "flow_safe",
            "--out",
            str(repaired_path),
            "--report",
            str(repair_report_path),
        ],
    )
    assert result.exit_code == 0
    assert repaired_path.exists()
    assert repair_report_path.exists()
