"""Address-policy regressions: DNS must not bypass CONNECT filtering."""

from __future__ import annotations

import ipaddress

import pytest
import trio
import trio.testing as tt

from trio_http_proxy import ProxyConfig, ProxyProtocolError, http, tunnel
from trio_http_proxy.protocol import is_target_allowed


@pytest.mark.parametrize(
    "address",
    [
        "100.64.0.1",
        "0.0.0.0",
        "224.0.0.1",
        "240.0.0.1",
        "::",
        "ff02::1",
        "::ffff:127.0.0.1",
        "::ffff:10.0.0.1",
        "::ffff:169.254.169.254",
        "64:ff9b::7f00:1",
        "64:ff9b::a9fe:a9fe",
        "2002:7f00:1::",
        "2002:a00:1::",
        "2001:0:4136:e378:8000:63bf:3fff:fdd2",
    ],
)
def test_non_public_addresses_rejected(address: str) -> None:
    assert not is_target_allowed(address, 443, ProxyConfig())
    assert is_target_allowed(address, 443, ProxyConfig(allow_private_networks=True))


@pytest.mark.parametrize(
    ("host", "pattern"),
    [
        ("EXAMPLE.COM.", "example.com"),
        ("example.com", "EXAMPLE.COM.:443"),
        ("2606:4700:4700::1111", "[2606:4700:4700:0:0:0:0:1111]:443"),
        ("localhost.", "LOCALHOST"),
        ("::1%1", "::1"),
        ("::1%lo", "[::1]:443"),
        ("::ffff:127.0.0.1", "127.0.0.1"),
        ("64:ff9b::7f00:1", "127.0.0.1"),
    ],
)
def test_canonical_target_matching(host: str, pattern: str) -> None:
    assert is_target_allowed(
        host, 443, ProxyConfig(allowed_targets=(pattern,), allow_private_networks=True)
    )
    assert not is_target_allowed(
        host, 443, ProxyConfig(denied_targets=(pattern,), allow_private_networks=True)
    )


def test_allowlist_does_not_override_private_guard() -> None:
    assert not is_target_allowed(
        "127.0.0.1", 443, ProxyConfig(allowed_targets=("127.0.0.1",))
    )
    assert not is_target_allowed("localhost.", 443, ProxyConfig())


def dns_answers(addresses: tuple[str, ...], port: int = 443) -> list[tuple]:
    return [
        (
            trio.socket.AF_INET6
            if ipaddress.ip_address(address).version == 6
            else trio.socket.AF_INET,
            trio.socket.SOCK_STREAM,
            trio.socket.IPPROTO_TCP,
            "",
            (address, port, 0, 0) if ":" in address else (address, port),
        )
        for address in addresses
    ]


@pytest.mark.parametrize(
    "addresses",
    [
        ("127.0.0.1",),
        ("169.254.169.254",),
        ("10.1.2.3",),
        ("::1",),
        ("::ffff:127.0.0.1",),
        ("64:ff9b::7f00:1",),
        ("64:ff9b::a9fe:a9fe",),
        ("8.8.8.8", "192.168.1.1"),
        ("::1", "2606:4700:4700::1111"),
    ],
)
async def test_dns_restrictions_reject_before_connect(
    addresses: tuple[str, ...], monkeypatch: pytest.MonkeyPatch
) -> None:
    async def resolve(*args, **kwargs):
        return dns_answers(addresses)

    async def connect(*args, **kwargs):
        pytest.fail("Denied DNS result must never reach TCP connect")

    monkeypatch.setattr(trio.socket, "getaddrinfo", resolve)
    monkeypatch.setattr(trio, "open_tcp_stream", connect)
    client, proxy_client = tt.memory_stream_pair()
    async with client, proxy_client:
        await client.send_all(
            b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com\r\n\r\n"
        )
        with pytest.raises(ProxyProtocolError):
            await tunnel(
                proxy_client, config=ProxyConfig(allowed_targets=("example.com",))
            )
        assert await client.receive_some(4096) == http.RESP_403_FORBIDDEN


@pytest.mark.parametrize(
    ("addresses", "config", "failed_first"),
    [
        (("8.8.8.8", "8.8.8.8"), ProxyConfig(), False),
        (
            ("8.8.8.8", "1.1.1.1"),
            ProxyConfig(allowed_targets=("example.com:443",)),
            True,
        ),
        (("2606:4700:4700::1111",), ProxyConfig(), False),
        (("127.0.0.1",), ProxyConfig(allow_private_networks=True), False),
    ],
)
async def test_connection_uses_only_vetted_numeric_addresses(
    addresses: tuple[str, ...],
    config: ProxyConfig,
    failed_first: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lookups = []
    connections = []
    upstream, peer = tt.memory_stream_pair()

    async def resolve(host, port, **kwargs):
        lookups.append((host, port))
        assert len(lookups) == 1, (
            "Do not re-resolve a hostname after vetting its addresses"
        )
        return dns_answers(addresses)

    async def connect(address, port):
        connections.append((address, port))
        if failed_first and len(connections) == 1:
            raise OSError("First public address unreachable")
        return upstream

    monkeypatch.setattr(trio.socket, "getaddrinfo", resolve)
    monkeypatch.setattr(trio, "open_tcp_stream", connect)
    client, proxy_client = tt.memory_stream_pair()
    async with client, proxy_client, upstream, peer:
        await client.send_all(
            b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com\r\n\r\n"
        )
        assert await tunnel(proxy_client, config=config) is upstream
        assert await client.receive_some(4096) == http.HTTP_200_CONNECTION_ESTABLISHED
    assert lookups == [("example.com", 443)]
    expected_addresses = tuple(dict.fromkeys(addresses))
    assert connections == [(address, 443) for address in expected_addresses]


@pytest.mark.parametrize("scoped", [False, True])
async def test_resolved_ip_denylist_applies_even_with_private_opt_out(
    monkeypatch: pytest.MonkeyPatch,
    scoped: bool,
) -> None:
    async def resolve(*args, **kwargs):
        if scoped:
            return [
                (
                    trio.socket.AF_INET6,
                    trio.socket.SOCK_STREAM,
                    6,
                    "",
                    ("::1", 443, 0, 1),
                )
            ]
        return dns_answers(("127.0.0.1",))

    async def connect(*args, **kwargs):
        pytest.fail("IP denylist must apply after resolving an allowed hostname")

    monkeypatch.setattr(trio.socket, "getaddrinfo", resolve)
    monkeypatch.setattr(trio, "open_tcp_stream", connect)
    client, proxy_client = tt.memory_stream_pair()
    async with client, proxy_client:
        await client.send_all(
            b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com\r\n\r\n"
        )
        with pytest.raises(ProxyProtocolError):
            await tunnel(
                proxy_client,
                config=ProxyConfig(
                    allow_private_networks=True, denied_targets=("127.0.0.1:443", "::1")
                ),
            )
        assert await client.receive_some(4096) == http.RESP_403_FORBIDDEN


@pytest.mark.parametrize("failure", ["empty", "dns", "connect"])
async def test_resolution_and_connection_failures_reply_502(
    failure: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def resolve(*args, **kwargs):
        if failure == "dns":
            raise trio.socket.gaierror("DNS failed")
        return [] if failure == "empty" else dns_answers(("8.8.8.8",))

    async def connect(*args, **kwargs):
        raise OSError("TCP failed")

    monkeypatch.setattr(trio.socket, "getaddrinfo", resolve)
    monkeypatch.setattr(trio, "open_tcp_stream", connect)
    client, proxy_client = tt.memory_stream_pair()
    async with client, proxy_client:
        await client.send_all(
            b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com\r\n\r\n"
        )
        with pytest.raises(OSError):
            await tunnel(proxy_client)
        assert await client.receive_some(4096) == http.HTTP_502_BAD_GATEWAY


async def test_dns_resolution_shares_connection_deadline(
    monkeypatch: pytest.MonkeyPatch,
    autojump_clock,
) -> None:
    async def resolve(*args, **kwargs):
        await trio.sleep_forever()

    monkeypatch.setattr(trio.socket, "getaddrinfo", resolve)
    client, proxy_client = tt.memory_stream_pair()
    async with client, proxy_client:
        await client.send_all(
            b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com\r\n\r\n"
        )
        with pytest.raises(trio.TooSlowError):
            await tunnel(proxy_client, config=ProxyConfig(dest_connect_timeout=1))
        assert await client.receive_some(4096) == http.HTTP_504_GATEWAY_TIMEOUT
