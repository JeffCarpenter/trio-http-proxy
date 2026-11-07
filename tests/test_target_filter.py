"""Unit and tunnel tests for target allowlist and SSRF filtering."""

from __future__ import annotations

import pytest
import trio
import trio.testing as tt

from trio_http_proxy import (
    ProxyConfig,
    ProxyProtocolError,
    http,
    tunnel,
)
from trio_http_proxy.protocol import is_target_allowed


def connect_request(target: bytes = b"example.com:443") -> bytes:
    """Generate a valid HTTP CONNECT request wire-format byte string."""
    return b"CONNECT " + target + b" HTTP/1.1\r\nHost: " + target + b"\r\n\r\n"


# ---------------------------------------------------------------------------
# Unit tests for is_target_allowed
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ip", ["127.0.0.1", "127.0.0.2", "::1", "[::1]"])
def test_loopback_ips_blocked_by_default(ip: str) -> None:
    cfg = ProxyConfig()
    assert is_target_allowed(ip, 443, cfg) is False


@pytest.mark.parametrize("ip", ["127.0.0.1", "::1", "[::1]"])
def test_loopback_ips_allowed_with_flag(ip: str) -> None:
    cfg = ProxyConfig(allow_private_networks=True)
    assert is_target_allowed(ip, 443, cfg) is True


@pytest.mark.parametrize(
    "ip", ["169.254.169.254", "169.254.1.1", "fe80::1", "[fe80::1]"]
)
def test_cloud_metadata_and_link_local_blocked_by_default(ip: str) -> None:
    cfg = ProxyConfig()
    assert is_target_allowed(ip, 80, cfg) is False


@pytest.mark.parametrize(
    "ip",
    [
        "10.0.0.1",
        "10.254.0.1",
        "172.16.0.1",
        "172.31.255.255",
        "192.168.0.1",
        "192.168.1.1",
        "fc00::1",
        "[fc00::1]",
    ],
)
def test_rfc1918_private_ips_blocked_by_default(ip: str) -> None:
    cfg = ProxyConfig()
    assert is_target_allowed(ip, 443, cfg) is False


@pytest.mark.parametrize(
    "ip",
    [
        "10.0.0.1",
        "172.16.0.1",
        "192.168.1.1",
        "fc00::1",
        "[fc00::1]",
    ],
)
def test_rfc1918_private_ips_allowed_with_flag(ip: str) -> None:
    cfg = ProxyConfig(allow_private_networks=True)
    assert is_target_allowed(ip, 443, cfg) is True


@pytest.mark.parametrize("ip", ["8.8.8.8", "1.1.1.1", "2606:4700:4700::1111"])
def test_public_ips_allowed_by_default(ip: str) -> None:
    cfg = ProxyConfig()
    assert is_target_allowed(ip, 443, cfg) is True


@pytest.mark.parametrize(
    "host", ["localhost", "LOCALHOST", "foo.localhost", "bar.baz.localhost"]
)
def test_localhost_blocked_by_default(host: str) -> None:
    cfg = ProxyConfig()
    assert is_target_allowed(host, 8080, cfg) is False


@pytest.mark.parametrize("host", ["localhost", "sub.localhost"])
def test_localhost_allowed_with_flag(host: str) -> None:
    cfg = ProxyConfig(allow_private_networks=True)
    assert is_target_allowed(host, 8080, cfg) is True


def test_allowed_targets_filtering() -> None:
    cfg = ProxyConfig(
        allowed_targets=("example.com", "api.example.com:8443", "8.8.8.8")
    )
    # Allowed domain on any port
    assert is_target_allowed("example.com", 443, cfg) is True
    assert is_target_allowed("example.com", 80, cfg) is True
    assert is_target_allowed("EXAMPLE.COM", 443, cfg) is True

    # Allowed host:port
    assert is_target_allowed("api.example.com", 8443, cfg) is True
    assert is_target_allowed("api.example.com", 443, cfg) is False

    # Allowed IP
    assert is_target_allowed("8.8.8.8", 53, cfg) is True

    # Unlisted domain
    assert is_target_allowed("other.com", 443, cfg) is False


def test_denied_targets_filtering() -> None:
    cfg = ProxyConfig(
        denied_targets=("evil.com", "restricted.org:80", "9.9.9.9"),
    )
    # Denied host on any port
    assert is_target_allowed("evil.com", 443, cfg) is False
    assert is_target_allowed("EVIL.COM", 80, cfg) is False

    # Denied host:port
    assert is_target_allowed("restricted.org", 80, cfg) is False
    assert is_target_allowed("restricted.org", 443, cfg) is True

    # Denied IP
    assert is_target_allowed("9.9.9.9", 53, cfg) is False

    # Unlisted host
    assert is_target_allowed("example.com", 443, cfg) is True


def test_denied_targets_override_allowed_targets() -> None:
    cfg = ProxyConfig(
        allowed_targets=("example.com", "internal.com"),
        denied_targets=("internal.com",),
    )
    assert is_target_allowed("example.com", 443, cfg) is True
    assert is_target_allowed("internal.com", 443, cfg) is False


# ---------------------------------------------------------------------------
# ProxyConfig.from_env parsing tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("val", ["1", "true", "True", "YES", "on", "On"])
def test_config_from_env_allow_private_networks_true(val: str) -> None:
    cfg = ProxyConfig.from_env({"TRIO_HTTP_PROXY_ALLOW_PRIVATE_NETWORKS": val})
    assert cfg.allow_private_networks is True


@pytest.mark.parametrize("val", ["0", "false", "no", "off", "invalid"])
def test_config_from_env_allow_private_networks_false(val: str) -> None:
    cfg = ProxyConfig.from_env({"TRIO_HTTP_PROXY_ALLOW_PRIVATE_NETWORKS": val})
    assert cfg.allow_private_networks is False


def test_config_from_env_targets_parsing() -> None:
    env = {
        "TRIO_HTTP_PROXY_ALLOWED_TARGETS": " Example.com , api.example.com:443 , ",
        "TRIO_HTTP_PROXY_DENIED_TARGETS": "evil.com,bad.org:80",
    }
    cfg = ProxyConfig.from_env(env)
    assert cfg.allowed_targets == ("example.com", "api.example.com:443")
    assert cfg.denied_targets == ("evil.com", "bad.org:80")


def test_config_from_env_legacy_names() -> None:
    env = {
        "PROXY_ALLOW_PRIVATE_NETWORKS": "yes",
        "PROXY_ALLOWED_TARGETS": "target1.com, target2.com",
        "PROXY_DENIED_TARGETS": "bad1.com, bad2.com",
    }
    cfg = ProxyConfig.from_env(env)
    assert cfg.allow_private_networks is True
    assert cfg.allowed_targets == ("target1.com", "target2.com")
    assert cfg.denied_targets == ("bad1.com", "bad2.com")


# ---------------------------------------------------------------------------
# Memory stream tunnel tests
# ---------------------------------------------------------------------------


async def test_tunnel_denied_target_returns_403_and_raises() -> None:
    proxy_client, test_client = tt.memory_stream_pair()
    cfg = ProxyConfig(denied_targets=("evil.com",))

    async def fake_connect(host: str, port: int) -> trio.abc.Stream:
        pytest.fail("Should not attempt upstream connection for denied target")

    await test_client.send_all(connect_request(target=b"evil.com:443"))

    with pytest.raises(ProxyProtocolError, match="forbidden by policy"):
        await tunnel(proxy_client, config=cfg, _connect=fake_connect)

    reply = await test_client.receive_some(4096)
    assert reply == http.RESP_403_FORBIDDEN


async def test_tunnel_loopback_target_blocked_by_default_returns_403() -> None:
    proxy_client, test_client = tt.memory_stream_pair()
    cfg = ProxyConfig()  # allow_private_networks is False by default

    async def fake_connect(host: str, port: int) -> trio.abc.Stream:
        pytest.fail("Should not attempt upstream connection for loopback IP")

    await test_client.send_all(connect_request(target=b"127.0.0.1:8080"))

    with pytest.raises(ProxyProtocolError, match="forbidden by policy"):
        await tunnel(proxy_client, config=cfg, _connect=fake_connect)

    reply = await test_client.receive_some(4096)
    assert reply == http.RESP_403_FORBIDDEN
