"""Configuration model for the minimal Trio HTTP proxy."""

from __future__ import annotations

import io
import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Self


def _lookup_env_int(
    names: tuple[str, ...],
    default: int,
    env: Mapping[str, str],
) -> int:
    for name in names:
        val = env.get(name)
        if val is not None and val != "":
            return int(val)
    return default


def _lookup_env_float(
    names: tuple[str, ...],
    default: float,
    env: Mapping[str, str],
) -> float:
    for name in names:
        val = env.get(name)
        if val is not None and val != "":
            return float(val)
    return default


def _lookup_env_str(
    names: tuple[str, ...],
    default: str,
    env: Mapping[str, str],
) -> str:
    for name in names:
        val = env.get(name)
        if val is not None and val != "":
            return val
    return default


@dataclass(frozen=True)
class ProxyConfig:
    """Immutable runtime configuration for the Trio HTTP proxy."""

    port: int = 8080
    bufmaxsz: int = io.DEFAULT_BUFFER_SIZE
    maxreqsz: int = 16384
    connect_read_timeout: float = 10.0
    dest_connect_timeout: float = 10.0
    pipe_idle_timeout: float = 300.0
    pipe_send_timeout: float = 30.0
    maxconns: int = 1000
    log_level: str = "INFO"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Self:
        """Construct ProxyConfig from environment variables or mapping."""
        mapping: Mapping[str, str] = os.environ if env is None else env
        return cls(
            port=_lookup_env_int(
                ("TRIO_HTTP_PROXY_PORT", "PORT"),
                8080,
                mapping,
            ),
            bufmaxsz=_lookup_env_int(
                ("TRIO_HTTP_PROXY_BUFMAXSZ", "BUFMAXSZ"),
                io.DEFAULT_BUFFER_SIZE,
                mapping,
            ),
            maxreqsz=_lookup_env_int(
                ("TRIO_HTTP_PROXY_MAXREQSZ", "MAXREQSZ"),
                16384,
                mapping,
            ),
            connect_read_timeout=_lookup_env_float(
                ("TRIO_HTTP_PROXY_CONNECT_READ_TIMEOUT", "CONNECT_READ_TIMEOUT"),
                10.0,
                mapping,
            ),
            dest_connect_timeout=_lookup_env_float(
                ("TRIO_HTTP_PROXY_DEST_CONNECT_TIMEOUT", "DEST_CONNECT_TIMEOUT"),
                10.0,
                mapping,
            ),
            pipe_idle_timeout=_lookup_env_float(
                ("TRIO_HTTP_PROXY_PIPE_IDLE_TIMEOUT", "PIPE_IDLE_TIMEOUT"),
                300.0,
                mapping,
            ),
            pipe_send_timeout=_lookup_env_float(
                ("TRIO_HTTP_PROXY_PIPE_SEND_TIMEOUT", "PIPE_SEND_TIMEOUT"),
                30.0,
                mapping,
            ),
            maxconns=_lookup_env_int(
                ("TRIO_HTTP_PROXY_MAXCONNS", "MAXCONNS"),
                1000,
                mapping,
            ),
            log_level=_lookup_env_str(
                ("TRIO_HTTP_PROXY_LOG_LEVEL", "LOG_LEVEL"),
                "INFO",
                mapping,
            ).upper(),
        )
