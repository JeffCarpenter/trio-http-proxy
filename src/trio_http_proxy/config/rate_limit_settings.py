"""Rate limiting configuration settings using pydantic-settings."""

from __future__ import annotations

from cashews.ttl import ttl_to_seconds
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class RateLimitSettings(BaseSettings):
    """Configuration settings for client IP rate limiting."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    rate_limit_enabled: bool = Field(
        default=False,
        description="Whether rate limiting per client IP is enabled.",
    )
    rate_limit: int = Field(
        default=0,
        ge=0,
        description="Maximum requests allowed within rate_limit_period (0 means unlimited).",
    )
    rate_limit_period: str = Field(
        default="60s",
        description="Window duration string over which requests are counted.",
    )
    rate_limit_ttl: str | None = Field(
        default=None,
        description="Ban TTL duration string applied once rate limit is exceeded.",
    )

    @field_validator("rate_limit_period")
    @classmethod
    def _validate_rate_limit_period(cls, value: str) -> str:
        try:
            parsed = ttl_to_seconds(value)
        except (ValueError, TypeError) as err:
            raise ValueError(
                f"Invalid duration for rate_limit_period: {value!r}"
            ) from err
        if parsed is None or parsed <= 0:
            raise ValueError(
                f"rate_limit_period must evaluate to > 0 seconds, got {value!r}"
            )
        return value

    @field_validator("rate_limit_ttl")
    @classmethod
    def _validate_rate_limit_ttl(cls, value: str | None) -> str | None:
        if value is None or (isinstance(value, str) and not value.strip()):
            return None
        try:
            parsed = ttl_to_seconds(value)
        except (ValueError, TypeError) as err:
            raise ValueError(f"Invalid duration for rate_limit_ttl: {value!r}") from err
        if parsed is None or parsed <= 0:
            raise ValueError(
                f"rate_limit_ttl must evaluate to > 0 seconds, got {value!r}"
            )
        return value


__all__ = ["RateLimitSettings"]
