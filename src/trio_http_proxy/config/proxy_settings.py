"""Proxy server configuration settings using pydantic-settings."""

from __future__ import annotations

from io import DEFAULT_BUFFER_SIZE
from typing import ClassVar

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ProxySettings(BaseSettings):
    """Configuration settings for proxy server networking, timeouts, and concurrency."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    VALID_LOG_LEVELS: ClassVar[frozenset[str]] = frozenset(
        {"CRITICAL", "FATAL", "ERROR", "WARN", "WARNING", "INFO", "DEBUG", "NOTSET"}
    )

    port: int = Field(
        default=8080,
        ge=0,
        le=65535,
        description="Port to listen on for incoming proxy connections.",
    )
    bufmaxsz: int = Field(
        default=DEFAULT_BUFFER_SIZE,
        ge=1,
        description="Maximum buffer size for data transfer.",
    )
    maxreqsz: int = Field(
        default=16384,
        ge=1,
        description="Maximum allowed request size before rejecting as RequestTooLargeError.",
    )
    connect_read_timeout: float = Field(
        default=10.0,
        gt=0.0,
        description="Timeout in seconds for reading client CONNECT request.",
    )
    dest_connect_timeout: float = Field(
        default=10.0,
        gt=0.0,
        description="Timeout in seconds for connecting to target destination.",
    )
    pipe_idle_timeout: float = Field(
        default=300.0,
        gt=0.0,
        description="Timeout in seconds when bidirectional pipe is completely idle.",
    )
    pipe_send_timeout: float = Field(
        default=30.0,
        gt=0.0,
        description="Timeout in seconds when sending data through pipe stalls.",
    )
    maxconns: int = Field(
        default=1000,
        ge=1,
        description="Maximum concurrent active proxy connections.",
    )
    log_level: str = Field(
        default="INFO",
        description="Logging level for proxy events.",
    )

    @field_validator("log_level")
    @classmethod
    def _validate_log_level(cls, value: str) -> str:
        norm = value.strip().upper()
        if norm not in cls.VALID_LOG_LEVELS:
            raise ValueError(
                f"Invalid log_level: {value!r}. Must be one of {sorted(cls.VALID_LOG_LEVELS)}"
            )
        return norm


__all__ = ["ProxySettings"]
