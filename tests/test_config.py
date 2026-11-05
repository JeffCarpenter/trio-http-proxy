"""Unit tests for ProxyConfig immutable configuration."""

import dataclasses
import io

import pytest

from trio_http_proxy.config import ProxyConfig


def test_proxy_config_defaults():
    config = ProxyConfig()
    assert config.port == 8080
    assert config.bufmaxsz == io.DEFAULT_BUFFER_SIZE
    assert config.maxreqsz == 16384
    assert config.connect_read_timeout == 10.0
    assert config.dest_connect_timeout == 10.0
    assert config.pipe_idle_timeout == 300.0
    assert config.pipe_send_timeout == 30.0
    assert config.maxconns == 1000
    assert config.log_level == "INFO"


def test_proxy_config_immutability():
    config = ProxyConfig()
    with pytest.raises(dataclasses.FrozenInstanceError):
        config.port = 9090  # type: ignore[misc]


def test_proxy_config_from_env_mapping():
    env = {
        "TRIO_HTTP_PROXY_PORT": "9090",
        "TRIO_HTTP_PROXY_BUFMAXSZ": "4096",
        "TRIO_HTTP_PROXY_MAXREQSZ": "8192",
        "TRIO_HTTP_PROXY_CONNECT_READ_TIMEOUT": "5.5",
        "TRIO_HTTP_PROXY_DEST_CONNECT_TIMEOUT": "6.5",
        "TRIO_HTTP_PROXY_PIPE_IDLE_TIMEOUT": "120.0",
        "TRIO_HTTP_PROXY_PIPE_SEND_TIMEOUT": "15.0",
        "TRIO_HTTP_PROXY_MAXCONNS": "500",
        "TRIO_HTTP_PROXY_LOG_LEVEL": "debug",
    }
    config = ProxyConfig.from_env(env)
    assert config.port == 9090
    assert config.bufmaxsz == 4096
    assert config.maxreqsz == 8192
    assert config.connect_read_timeout == 5.5
    assert config.dest_connect_timeout == 6.5
    assert config.pipe_idle_timeout == 120.0
    assert config.pipe_send_timeout == 15.0
    assert config.maxconns == 500
    assert config.log_level == "DEBUG"


def test_proxy_config_from_env_legacy_names():
    env = {
        "PORT": "8888",
        "BUFMAXSZ": "2048",
        "MAXREQSZ": "4096",
        "CONNECT_READ_TIMEOUT": "3.0",
        "DEST_CONNECT_TIMEOUT": "4.0",
        "PIPE_IDLE_TIMEOUT": "60.0",
        "PIPE_SEND_TIMEOUT": "10.0",
        "MAXCONNS": "250",
        "LOG_LEVEL": "warn",
    }
    config = ProxyConfig.from_env(env)
    assert config.port == 8888
    assert config.bufmaxsz == 2048
    assert config.maxreqsz == 4096
    assert config.connect_read_timeout == 3.0
    assert config.dest_connect_timeout == 4.0
    assert config.pipe_idle_timeout == 60.0
    assert config.pipe_send_timeout == 10.0
    assert config.maxconns == 250
    assert config.log_level == "WARN"


def test_proxy_config_precedence():
    env = {
        "TRIO_HTTP_PROXY_PORT": "9999",
        "PORT": "8888",
    }
    config = ProxyConfig.from_env(env)
    assert config.port == 9999
