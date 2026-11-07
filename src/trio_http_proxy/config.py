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


def _lookup_env_bool(
    names: tuple[str, ...],
    default: bool,
    env: Mapping[str, str],
) -> bool:
    for name in names:
        val = env.get(name)
        if val is not None and val != "":
            return val.strip().lower() in ("1", "true", "yes", "on")
    return default


def _lookup_env_tuple(
    names: tuple[str, ...],
    default: tuple[str, ...],
    env: Mapping[str, str],
) -> tuple[str, ...]:
    for name in names:
        val = env.get(name)
        if val is not None and val != "":
            return tuple(
                item.strip().lower() for item in val.split(",") if item.strip()
            )
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
    shutdown_timeout: float = 15.0
    allow_private_networks: bool = False
    allowed_targets: tuple[str, ...] = ()
    denied_targets: tuple[str, ...] = ()

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
            allow_private_networks=_lookup_env_bool(
                (
                    "TRIO_HTTP_PROXY_ALLOW_PRIVATE_NETWORKS",
                    "PROXY_ALLOW_PRIVATE_NETWORKS",
                ),
                False,
                mapping,
            ),
            allowed_targets=_lookup_env_tuple(
                ("TRIO_HTTP_PROXY_ALLOWED_TARGETS", "PROXY_ALLOWED_TARGETS"),
                (),
                mapping,
            ),
            denied_targets=_lookup_env_tuple(
                ("TRIO_HTTP_PROXY_DENIED_TARGETS", "PROXY_DENIED_TARGETS"),
                (),
                mapping,
            ),
            shutdown_timeout=_lookup_env_float(
                (
                    "TRIO_HTTP_PROXY_SHUTDOWN_TIMEOUT",
                    "PROXY_SHUTDOWN_TIMEOUT",
                    "SHUTDOWN_TIMEOUT",
                ),
                15.0,
                mapping,
            ),
        )
