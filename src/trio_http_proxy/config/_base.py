"""Shared primitives for trio_http_proxy settings models.

Defines the common BaseSettings configuration (frozen, strict-init, dotenv
filtering), the canonical ``TRIO_HTTP_PROXY_`` env-name aliasing helper, and
the single source of truth for duration-string parsing (cashews TTL syntax).
"""

from __future__ import annotations

from pydantic import AliasChoices
from pydantic_settings import BaseSettings, SettingsConfigDict

ENV_PREFIX = "TRIO_HTTP_PROXY_"


def prefixed(*names: str) -> AliasChoices:
    """Env aliases for a field: canonical prefixed name first, legacy name second.

    ``prefixed("circuit_breaker_period")`` yields the aliases
    ``TRIO_HTTP_PROXY_CIRCUIT_BREAKER_PERIOD`` (canonical, wins) and
    ``CIRCUIT_BREAKER_PERIOD`` (legacy). Field-name initialization keeps working
    via ``populate_by_name``.
    """
    ups = [name.upper() for name in names]
    return AliasChoices(*(f"{ENV_PREFIX}{u}" for u in ups), *ups)


def parse_duration_seconds(
    value: str, *, field: str, allow_zero: bool = False
) -> float:
    """Parse a cashews TTL duration string into seconds, exactly once, everywhere.

    Accepts integer amounts with s/m/h/d suffixes (e.g. ``"60s"``, ``"5m"``,
    ``"1h"``, ``"2d"``). Empty strings mean zero. Raises ``ValueError`` with the
    field name in the message on any invalid or (unless ``allow_zero``) zero
    duration.
    """
    from cashews.ttl import ttl_to_seconds

    stripped = value.strip()
    if not stripped:
        seconds = 0.0
    else:
        try:
            parsed = ttl_to_seconds(stripped)
        except (ValueError, TypeError) as err:
            raise ValueError(
                f"Invalid duration for {field}: {value!r}. "
                "Expected an integer amount with an s/m/h/d suffix (e.g. '60s', '5m')."
            ) from err
        if parsed is None:
            raise ValueError(f"Invalid duration for {field}: {value!r}.")
        seconds = float(parsed)
    if seconds < 0:
        raise ValueError(f"{field} must evaluate to >= 0 seconds, got {value!r}.")
    if seconds == 0 and not allow_zero:
        raise ValueError(f"{field} must evaluate to > 0 seconds, got {value!r}.")
    return seconds


def normalize_optional_duration(value: str | None, *, field: str) -> str | None:
    """Normalize an optional duration string; ``None``/``""``/``"0s"`` mean disabled."""
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    seconds = parse_duration_seconds(stripped, field=field, allow_zero=True)
    return stripped if seconds > 0 else None


def validate_duration_string(
    value: str, *, field: str, allow_zero: bool = False
) -> str:
    """Strip, validate, and return a duration string; enforces positivity unless allow_zero."""
    stripped = value.strip()
    if not stripped:
        if allow_zero:
            return "0s"
        raise ValueError(f"{field} must be a positive duration, got {value!r}")
    parse_duration_seconds(stripped, field=field, allow_zero=allow_zero)
    return stripped


class EnvSettings(BaseSettings):
    """Base class for env-aware settings groups.

    - frozen: validated settings are immutable shared values.
    - extra="forbid": typo'd init kwargs fail fast (unknown env/dotenv vars are
      still ignored, via dotenv_filtering + the env source only matching fields).
    - case-insensitive env matching; ``.env`` (CWD-relative) supported.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="forbid",
        populate_by_name=True,
        dotenv_filtering="only_existing",
        frozen=True,
    )


__all__ = [
    "ENV_PREFIX",
    "EnvSettings",
    "normalize_optional_duration",
    "parse_duration_seconds",
    "prefixed",
    "validate_duration_string",
]

