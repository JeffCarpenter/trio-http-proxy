"""Circuit breaker configuration settings (backward-compatibility alias for Settings)."""

from __future__ import annotations

from .settings import CircuitBreakerSettings, Settings

__all__ = ["CircuitBreakerSettings", "Settings"]
