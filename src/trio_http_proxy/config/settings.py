"""Typed composition and loading of proxy configuration groups."""

from __future__ import annotations

from functools import cache
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

from ._base import EnvSettings
from .cache_settings import CacheSettings
from .circuit_breaker_settings import CircuitBreakerSettings
from .proxy_settings import ProxySettings
from .rate_limit_settings import RateLimitSettings


class Settings(BaseModel):
    """Immutable application configuration composed from explicit groups."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    proxy: ProxySettings
    cache: CacheSettings
    circuit_breaker: CircuitBreakerSettings
    rate_limit: RateLimitSettings


def _load_group[SettingsGroup: EnvSettings](
    group_type: type[SettingsGroup],
    *,
    env_file: str | Path | None,
    env_file_encoding: str | None,
) -> SettingsGroup:
    source_options: dict[str, Any] = {
        "_env_file": env_file,
        "_env_file_encoding": env_file_encoding,
    }
    return group_type(**source_options)


def load_settings(
    *,
    env_file: str | Path | None = Path(".env"),
    env_file_encoding: str | None = "utf-8",
) -> Settings:
    """Load every settings group from the same environment and dotenv sources."""
    source_options = {
        "env_file": env_file,
        "env_file_encoding": env_file_encoding,
    }
    return Settings(
        proxy=_load_group(ProxySettings, **source_options),
        cache=_load_group(CacheSettings, **source_options),
        circuit_breaker=_load_group(CircuitBreakerSettings, **source_options),
        rate_limit=_load_group(RateLimitSettings, **source_options),
    )


@cache
def get_settings() -> Settings:
    """Return the lazily loaded, process-wide application settings."""
    return load_settings()


reset_settings_cache = get_settings.cache_clear


__all__ = ["Settings", "get_settings", "load_settings", "reset_settings_cache"]
