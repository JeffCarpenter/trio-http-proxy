"""Circuit breaker configuration settings using pydantic-settings."""

from __future__ import annotations

from typing import Any

from pydantic import Field, PrivateAttr, field_validator

from ._base import EnvSettings, parse_duration_seconds, prefixed


class CircuitBreakerSettings(EnvSettings):
    """Configuration settings for destination-based circuit breaking."""

    circuit_breaker_enabled: bool = Field(
        default=True,
        description="Whether destination circuit breaking is active.",
        validation_alias=prefixed("circuit_breaker_enabled"),
    )
    circuit_breaker_errors_rate: int = Field(
        default=50,
        ge=1,
        le=99,
        description="Percentage error rate threshold (1-99) tripping breaker to OPEN.",
        validation_alias=prefixed("circuit_breaker_errors_rate"),
    )
    circuit_breaker_period: str = Field(
        default="60s",
        description="Duration window string over which errors are counted.",
        validation_alias=prefixed("circuit_breaker_period"),
    )
    circuit_breaker_ttl: str = Field(
        default="30s",
        description="Cool-down duration string before probing in HALF_OPEN.",
        validation_alias=prefixed("circuit_breaker_ttl"),
    )
    circuit_breaker_min_calls: int = Field(
        default=3,
        ge=1,
        description="Minimum calls required in window before tripping.",
        validation_alias=prefixed("circuit_breaker_min_calls"),
    )
    circuit_breaker_max_destinations: int = Field(
        default=10000,
        ge=1,
        description="Maximum destinations tracked concurrently in LRU cache.",
        validation_alias=prefixed("circuit_breaker_max_destinations"),
    )

    _period_seconds: float = PrivateAttr(default=60.0)
    _ttl_seconds: float = PrivateAttr(default=30.0)

    @field_validator("circuit_breaker_period")
    @classmethod
    def _validate_circuit_breaker_period(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError(
                f"circuit_breaker_period must be a positive duration, got {value!r}"
            )
        parse_duration_seconds(stripped, field="circuit_breaker_period")
        return stripped

    @field_validator("circuit_breaker_ttl")
    @classmethod
    def _validate_circuit_breaker_ttl(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError(
                f"circuit_breaker_ttl must be a positive duration, got {value!r}"
            )
        parse_duration_seconds(stripped, field="circuit_breaker_ttl")
        return stripped

    def model_post_init(self, context: Any, /) -> None:
        object.__setattr__(
            self,
            "_period_seconds",
            parse_duration_seconds(
                self.circuit_breaker_period, field="circuit_breaker_period"
            ),
        )
        object.__setattr__(
            self,
            "_ttl_seconds",
            parse_duration_seconds(
                self.circuit_breaker_ttl, field="circuit_breaker_ttl"
            ),
        )

    @property
    def circuit_breaker_period_seconds(self) -> float:
        """Error-counting window in seconds."""
        return self._period_seconds

    @property
    def circuit_breaker_ttl_seconds(self) -> float:
        """Open-circuit cool-down in seconds."""
        return self._ttl_seconds


__all__ = ["CircuitBreakerSettings"]
