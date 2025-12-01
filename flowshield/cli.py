"""CLI entrypoint for FlowShield."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from . import profiles as profiles_module
from .core import FlowShield
from .io import load_dataframe, save_dataframe
from .profile import ConstraintProfile
from .schema import FeatureSchema

app = typer.Typer(help="FlowShield CLI")


@app.command()
def init_schema(columns: str, out: str = "schema.json") -> None:
    """Initialize a schema template from comma-separated column names."""

    column_list = [c.strip() for c in columns.split(",") if c.strip()]
    schema = FeatureSchema.from_columns(column_list, defaults={"dtype": "float", "non_negative": True})
    schema.to_json(out)
    typer.echo(f"Schema written to {out}")


@app.command()
def validate(data: str, schema: str, profile: str, out: str = "report.md", sample_limit: Optional[int] = None) -> None:
    """Validate a dataset using a schema and profile."""

    df = load_dataframe(data)
    schema_obj = FeatureSchema.from_json(schema)
    profile_obj: ConstraintProfile = getattr(profiles_module, profile)
    flow = FlowShield()
    report = flow.validate(df, schema_obj, profile_obj, sample_limit=sample_limit)
    if out.endswith(".json"):
        Path(out).write_text(report._as_json())
    else:
        Path(out).write_text(report._as_markdown())
    typer.echo(f"Validation report written to {out}")
    if report.violations_by_severity.get("error", 0) > 0:
        raise typer.Exit(code=2)


@app.command()
def repair(
    data: str,
    schema: str,
    profile: str,
    out: str = "repaired.csv",
    report: str = "repair_report.json",
) -> None:
    """Repair a dataset and emit a repair report."""

    df = load_dataframe(data)
    schema_obj = FeatureSchema.from_json(schema)
    profile_obj: ConstraintProfile = getattr(profiles_module, profile)
    flow = FlowShield()
    flow.fit_repair_stats(df, schema_obj)
    repaired, repair_report = flow.repair(df, schema_obj, profile_obj)
    save_dataframe(repaired, out)
    if report.endswith(".md"):
        Path(report).write_text(repair_report._as_markdown())
    else:
        Path(report).write_text(repair_report._as_json())
    typer.echo(f"Repaired data written to {out}; report written to {report}")


@app.command()
def profiles() -> None:  # type: ignore[override]
    """List built-in profiles."""

    for name in ("flow_safe", "strict_flow", "telemetry_noisy"):
        profile_obj: ConstraintProfile = getattr(profiles_module, name)
        typer.echo(f"{profile_obj.name}: {profile_obj.description}")


if __name__ == "__main__":
    app()
