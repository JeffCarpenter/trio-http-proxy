"""Circuit breaker configuration settings using pydantic-settings."""

from __future__ import annotations

from cashews.ttl import ttl_to_seconds
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class CircuitBreakerSettings(BaseSettings):
    """Configuration settings for destination-based circuit breaking."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    circuit_breaker_enabled: bool = Field(
        default=True,
        description="Whether destination circuit breaking is active.",
    )
    circuit_breaker_errors_rate: int = Field(
        default=50,
        ge=1,
        le=99,
        description="Percentage error rate threshold (1-99) tripping breaker to OPEN.",
    )
    circuit_breaker_period: str = Field(
        default="60s",
        description="Duration window string over which errors are counted.",
    )
    circuit_breaker_ttl: str = Field(
        default="30s",
        description="Cool-down duration string before probing in HALF_OPEN.",
    )
    circuit_breaker_min_calls: int = Field(
        default=3,
        ge=1,
        description="Minimum calls required in window before tripping.",
    )
    circuit_breaker_max_destinations: int = Field(
        default=10000,
        ge=1,
        description="Maximum destinations tracked concurrently in LRU cache.",
    )

    @field_validator("circuit_breaker_period")
    @classmethod
    def _validate_circuit_breaker_period(cls, value: str) -> str:
        try:
            parsed = ttl_to_seconds(value)
        except (ValueError, TypeError) as err:
            raise ValueError(
                f"Invalid duration for circuit_breaker_period: {value!r}"
            ) from err
        if parsed is None or parsed <= 0:
            raise ValueError(
                f"circuit_breaker_period must evaluate to > 0 seconds, got {value!r}"
            )
        return value

    @field_validator("circuit_breaker_ttl")
    @classmethod
    def _validate_circuit_breaker_ttl(cls, value: str) -> str:
        try:
            parsed = ttl_to_seconds(value)
        except (ValueError, TypeError) as err:
            raise ValueError(
                f"Invalid duration for circuit_breaker_ttl: {value!r}"
            ) from err
        if parsed is None or parsed <= 0:
            raise ValueError(
                f"circuit_breaker_ttl must evaluate to > 0 seconds, got {value!r}"
            )
        return value


__all__ = ["CircuitBreakerSettings"]
