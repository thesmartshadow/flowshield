"""Custom exceptions for FlowShield."""

class FlowShieldError(Exception):
    """Base exception for FlowShield errors."""


class SchemaError(FlowShieldError):
    """Raised when schema validation fails."""


class ProfileError(FlowShieldError):
    """Raised when profile validation fails."""


class ValidationError(FlowShieldError):
    """Raised for validation failures that block further processing."""
