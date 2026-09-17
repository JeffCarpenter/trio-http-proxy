"""Proxy server configuration settings using pydantic-settings."""

from __future__ import annotations

from io import DEFAULT_BUFFER_SIZE
from typing import ClassVar

from pydantic import Field, FiniteFloat, field_validator

from ._base import EnvSettings, prefixed


class ProxySettings(EnvSettings):
    """Configuration settings for proxy server networking, timeouts, and concurrency.

    Environment names: ``TRIO_HTTP_PROXY_<FIELD>`` (canonical) or the legacy
    unprefixed ``<FIELD>`` (e.g. ``PORT``); the prefixed name wins when both are set.
    """

    VALID_LOG_LEVELS: ClassVar[frozenset[str]] = frozenset(
        {"CRITICAL", "FATAL", "ERROR", "WARN", "WARNING", "INFO", "DEBUG", "NOTSET"}
    )

    port: int = Field(
        default=8080,
        ge=0,  # 0 deliberately allowed: OS selects an ephemeral port
        le=65535,
        description="Port to listen on for incoming proxy connections.",
        validation_alias=prefixed("port"),
    )
    bufmaxsz: int = Field(
        default=DEFAULT_BUFFER_SIZE,
        ge=1,
        description="Maximum buffer size for data transfer.",
        validation_alias=prefixed("bufmaxsz"),
    )
    maxreqsz: int = Field(
        default=16384,
        ge=1,
        description="Maximum allowed request size before rejecting as RequestTooLargeError.",
        validation_alias=prefixed("maxreqsz"),
    )
    connect_read_timeout: FiniteFloat = Field(
        default=10.0,
        gt=0.0,
        description="Timeout in seconds for reading client CONNECT request.",
        validation_alias=prefixed("connect_read_timeout"),
    )
    dest_connect_timeout: FiniteFloat = Field(
        default=10.0,
        gt=0.0,
        description="Timeout in seconds for connecting to target destination.",
        validation_alias=prefixed("dest_connect_timeout"),
    )
    pipe_idle_timeout: FiniteFloat = Field(
        default=300.0,
        gt=0.0,
        description="Timeout in seconds when bidirectional pipe is completely idle.",
        validation_alias=prefixed("pipe_idle_timeout"),
    )
    pipe_send_timeout: FiniteFloat = Field(
        default=30.0,
        gt=0.0,
        description="Timeout in seconds when sending data through pipe stalls.",
        validation_alias=prefixed("pipe_send_timeout"),
    )
    maxconns: int = Field(
        default=1000,
        ge=1,
        description="Maximum concurrent active proxy connections.",
        validation_alias=prefixed("maxconns"),
    )
    log_level: str = Field(
        default="INFO",
        description="Logging level for proxy events.",
        validation_alias=prefixed("log_level"),
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
