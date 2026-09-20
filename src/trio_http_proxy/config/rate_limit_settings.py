"""Rate limiting configuration settings (backward-compatibility alias for Settings)."""

from __future__ import annotations

from .settings import RateLimitSettings, Settings

__all__ = ["RateLimitSettings", "Settings"]
