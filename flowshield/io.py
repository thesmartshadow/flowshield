"""Data loading utilities."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import pandas as pd

from .exceptions import FlowShieldError


def load_dataframe(path: str | Path) -> pd.DataFrame:
    ext = Path(path).suffix.lower()
    if ext in {".csv"}:
        return pd.read_csv(path)
    if ext in {".parquet"}:
        return pd.read_parquet(path)
    raise FlowShieldError(f"Unsupported data format: {ext}")


def save_dataframe(df: pd.DataFrame, path: str | Path) -> None:
    ext = Path(path).suffix.lower()
    if ext in {".csv"}:
        df.to_csv(path, index=False)
        return
    if ext in {".parquet"}:
        df.to_parquet(path, index=False)
        return
    raise FlowShieldError(f"Unsupported output format: {ext}")
