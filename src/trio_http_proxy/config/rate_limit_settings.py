"""Rate limiting configuration settings using pydantic-settings."""

from __future__ import annotations

from typing import Any

from pydantic import Field, PrivateAttr, field_validator

from ._base import (
    EnvSettings,
    normalize_optional_duration,
    parse_duration_seconds,
    prefixed,
)


class RateLimitSettings(EnvSettings):
    """Configuration settings for client IP rate limiting.

    ``rate_limit_ttl`` is optional: ``None``, ``""``, or ``"0s"`` all mean
    disabled (fall back to ``rate_limit_period`` semantics downstream).
    """

    rate_limit_enabled: bool = Field(
        default=False,
        description="Whether rate limiting per client IP is enabled.",
        validation_alias=prefixed("rate_limit_enabled"),
    )
    rate_limit: int = Field(
        default=0,
        ge=0,
        description="Maximum requests allowed within rate_limit_period (0 means unlimited).",
        validation_alias=prefixed("rate_limit"),
    )
    rate_limit_period: str = Field(
        default="60s",
        description="Window duration string over which requests are counted.",
        validation_alias=prefixed("rate_limit_period"),
    )
    rate_limit_ttl: str | None = Field(
        default=None,
        description="Ban TTL duration string applied once rate limit is exceeded ('0s'/'': disabled).",
        validation_alias=prefixed("rate_limit_ttl"),
    )

    _period_seconds: float = PrivateAttr(default=60.0)
    _ttl_seconds: float | None = PrivateAttr(default=None)

    @field_validator("rate_limit_period")
    @classmethod
    def _validate_rate_limit_period(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError(
                f"rate_limit_period must be a positive duration, got {value!r}"
            )
        parse_duration_seconds(stripped, field="rate_limit_period")
        return stripped

    @field_validator("rate_limit_ttl")
    @classmethod
    def _validate_rate_limit_ttl(cls, value: str | None) -> str | None:
        return normalize_optional_duration(value, field="rate_limit_ttl")

    def model_post_init(self, context: Any, /) -> None:
        object.__setattr__(
            self,
            "_period_seconds",
            parse_duration_seconds(self.rate_limit_period, field="rate_limit_period"),
        )
        object.__setattr__(
            self,
            "_ttl_seconds",
            (
                parse_duration_seconds(self.rate_limit_ttl, field="rate_limit_ttl")
                if self.rate_limit_ttl is not None
                else None
            ),
        )

    @property
    def rate_limit_period_seconds(self) -> float:
        """Rate-limit window in seconds."""
        return self._period_seconds

    @property
    def rate_limit_ttl_seconds(self) -> float | None:
        """Ban TTL in seconds; ``None`` means disabled."""
        return self._ttl_seconds


__all__ = ["RateLimitSettings"]
