"""Cache backend and namespace configuration (backward-compatibility alias for Settings)."""

from __future__ import annotations

from ._base import (
    NAMESPACE_PATTERN,
    normalize_failure_policy,
    validate_namespace,
)
from .settings import CacheSettings, Settings

__all__ = [
    "NAMESPACE_PATTERN",
    "CacheSettings",
    "Settings",
    "normalize_failure_policy",
    "validate_namespace",
]
