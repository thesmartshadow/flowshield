"""Schema definitions for FlowShield."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, ValidationError as PydanticValidationError, model_validator

from .exceptions import SchemaError

SUPPORTED_DTYPES = {"float", "int", "bool", "categorical"}


class FeatureColumn(BaseModel):
    """Definition for a single feature column."""

    name: str
    dtype: str = Field(..., description="Column dtype: float|int|bool|categorical")
    minimum: Optional[float] = Field(None, description="Minimum allowed value")
    maximum: Optional[float] = Field(None, description="Maximum allowed value")
    integer_only: bool = False
    non_negative: bool = False
    nullable: bool = False
    unit: Optional[str] = None
    description: Optional[str] = None
    categories: Optional[List[str]] = None
    dependencies: Optional[List[str]] = None

    @model_validator(mode="after")
    def validate_consistency(self) -> "FeatureColumn":
        if not self.name:
            raise SchemaError("Column name must be non-empty")
        if self.dtype not in SUPPORTED_DTYPES:
            raise SchemaError(f"Unsupported dtype '{self.dtype}' for column {self.name}")
        if self.minimum is not None and self.maximum is not None:
            if self.minimum > self.maximum:
                raise SchemaError(
                    f"Minimum greater than maximum for column {self.name}: {self.minimum}>{self.maximum}"
                )
        if self.dtype == "categorical" and self.categories is not None:
            if len(set(self.categories)) != len(self.categories):
                raise SchemaError(f"Duplicate categories for column {self.name}")
        return self


class FeatureSchema(BaseModel):
    """Schema describing feature columns."""

    columns: List[FeatureColumn]

    @model_validator(mode="after")
    def validate_schema(self) -> "FeatureSchema":
        names = [c.name for c in self.columns]
        if len(set(names)) != len(names):
            raise SchemaError("Feature columns must have unique names")
        return self

    @classmethod
    def from_columns(cls, columns: List[str], defaults: Optional[Dict[str, Any]] = None) -> "FeatureSchema":
        defaults = defaults or {}
        specs = [FeatureColumn(name=col, **defaults) for col in columns]
        return cls(columns=specs)

    @classmethod
    def from_json(cls, path: str | Path) -> "FeatureSchema":
        try:
            data = Path(path).read_text()
            return cls.model_validate_json(data)
        except PydanticValidationError as exc:
            raise SchemaError(str(exc)) from exc

    def to_json(self, path: str | Path) -> None:
        Path(path).write_text(self.model_dump_json(indent=2))

    def get_column(self, name: str) -> FeatureColumn:
        for col in self.columns:
            if col.name == name:
                return col
        raise SchemaError(f"Column {name} not found in schema")

    def column_names(self) -> List[str]:
        return [c.name for c in self.columns]
