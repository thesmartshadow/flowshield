# FlowShield

FlowShield is a constraints, validation, and repair toolkit for tabular network-flow feature vectors used in machine-learning Intrusion Detection Systems (IDS). It keeps feature vectors realistic after preprocessing, augmentation, adversarial simulation, data integration, or telemetry sampling artifacts. FlowShield is defensive and educational only.

## Why FlowShield
Network-flow features often drift or become inconsistent during data engineering. FlowShield helps researchers and engineers maintain realistic telemetry by:
- Declaring a feature schema with datatypes, ranges, and expectations.
- Applying constraint profiles that encode validation and repair strategies.
- Producing audit-ready reports for reproducible experiments.

## Installation
```bash
pip install .
```

Requires Python 3.11+, pandas, numpy, pydantic v2, and typer. No network calls are made.

## Quickstart
1. Initialize a schema template:
```bash
flowshield init-schema --columns duration,packets,bytes,p50,p95 --out schema.json
```

2. Validate a dataset:
```bash
flowshield validate --data flows.csv --schema schema.json --profile flow_safe --out report.md
```
Exit code is `2` when any error-level violations are detected.

3. Repair a dataset:
```bash
flowshield repair --data flows.csv --schema schema.json --profile flow_safe --out repaired.csv --report repair_report.json
```

## Profiles and relation rules
FlowShield ships with built-in profiles:
- **flow_safe**: Conservative clipping and median imputation.
- **strict_flow**: Strict bounds; rejects missing values; enforces percentile ordering.
- **telemetry_noisy**: Aggressive repairs and imputation for noisy telemetry.

Relation rules capture cross-feature expectations such as sum bounds, order relations, ratios, conditional expectations, and non-decreasing percentile groups.

## Safety note
FlowShield is designed for defensive validation and repair of IDS features. It does not offer offensive capabilities and performs no network access.

## Author
Zaid Abdullah Khalil
