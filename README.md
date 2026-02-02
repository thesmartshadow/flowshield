# FlowShield

**FlowShield** is a high-performance framework designed for the validation, constraint enforcement, and automated repair of tabular network-flow feature vectors. Engineered specifically for Machine Learning-based Intrusion Detection Systems (IDS), it ensures that feature vectors remain architecturally sound and statistically realistic following data augmentation, adversarial simulations, or telemetry sampling artifacts.

## Core Value Proposition

In the lifecycle of IDS data engineering, network-flow features often suffer from drift or logical inconsistencies. FlowShield provides a robust layer of defense by:

* **Schema Enforcement:** Defining rigorous data types, value ranges, and statistical expectations.
* **Constraint Profiles:** Implementing programmable validation and repair strategies to maintain telemetry realism.
* **Auditability:** Generating comprehensive, audit-ready reports to ensure reproducibility in scientific experiments and production pipelines.

---

## Technical Specifications & Installation

FlowShield is built for local execution with zero external network dependencies, ensuring data privacy and operational integrity.

**Requirements:** Python 3.11+, Pandas, NumPy, Pydantic v2, Typer.

```bash
pip install .

```

---

## Operational Workflow

### 1. Schema Initialization

Generate a standardized schema template defining the feature space:

```bash
flowshield init-schema "duration,packets,bytes,p50,p95" --out schema.json

```

### 2. Rigorous Validation

Evaluate datasets against defined constraints. The system returns an exit code `2` upon detecting critical violations to facilitate CI/CD integration:

```bash
flowshield validate examples/flows.csv schema.json flow_safe --out report.md

```

### 3. Automated Repair

Execute heuristic-based repairs to sanitize inconsistent telemetry and restore feature logic:

```bash
flowshield repair examples/flows.csv schema.json flow_safe --out repaired.csv --report repair_report.json

```

---

## Logic Profiles & Relational Constraints

FlowShield utilizes predefined profiles to handle varying levels of data integrity:

| Profile | Description | Strategy |
| --- | --- | --- |
| `flow_safe` | Standard baseline | Conservative clipping and median imputation. |
| `strict_flow` | High-fidelity | Enforces absolute bounds; rejects nulls; strict ordering. |
| `telemetry_noisy` | Robust recovery | Aggressive repairs and imputation for unstable telemetry. |

The engine validates **Relation Rules**, which govern complex dependencies such as sum-bounds, ratios, conditional expectations, and monotonic percentile sequences (e.g., ).

---

## Compliance and Safety Note

FlowShield is strictly a defensive utility for IDS feature engineering and academic research. It contains no offensive capabilities and operates entirely within an isolated local environment with no outbound network access.

**Author:** Ali Firas - thesmartshadow
