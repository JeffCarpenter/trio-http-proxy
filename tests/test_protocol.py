"""Unit tests for HTTP CONNECT protocol parsing and RFC 7231 §4.3.6 validation."""

import pytest
import trio
import trio.testing

from trio_http_proxy.errors import ProxyProtocolError, RequestTooLargeError
from trio_http_proxy.protocol import parse_connect_target
from trio_http_proxy.proxy import process_as_connect_request


def test_parse_connect_target_valid():
    assert parse_connect_target(b"example.com:443") == ("example.com", 443)
    assert parse_connect_target(b"127.0.0.1:8080") == ("127.0.0.1", 8080)
    assert parse_connect_target(b"[::1]:8443") == ("::1", 8443)
    assert parse_connect_target(b"[2001:db8::1]:443") == ("2001:db8::1", 443)


@pytest.mark.parametrize(
    "target",
    [
        b"user:pass@example.com:443",
        b"example.com:443/path",
        b"example.com:443?query=1",
        b"example.com:443#fragment",
        b"example.com",
        b":443",
        b"example.com:notaport",
        b"example.com:0",
        b"example.com:65536",
        b"example.com:-1",
        b"\xff\xfe:443",
    ],
)
def test_parse_connect_target_invalid(target: bytes):
    with pytest.raises(ProxyProtocolError):
        parse_connect_target(target)


async def test_process_as_connect_request_success():
    client_tx, proxy_rx = trio.testing.memory_stream_pair()
    await client_tx.send_all(
        b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n"
    )
    host, port, trailing = await process_as_connect_request(proxy_rx)
    assert host == "example.com"
    assert port == 443
    assert trailing == b""


async def test_process_as_connect_request_with_trailing_data():
    client_tx, proxy_rx = trio.testing.memory_stream_pair()
    payload = (
        b"CONNECT 127.0.0.1:9000 HTTP/1.1\r\nHost: 127.0.0.1:9000\r\n\r\nEARLY_DATA"
    )
    await client_tx.send_all(payload)
    host, port, trailing = await process_as_connect_request(proxy_rx)
    assert host == "127.0.0.1"
    assert port == 9000
    assert trailing == b"EARLY_DATA"


async def test_process_as_connect_request_non_connect_method():
    client_tx, proxy_rx = trio.testing.memory_stream_pair()
    await client_tx.send_all(b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
    with pytest.raises(ProxyProtocolError):
        await process_as_connect_request(proxy_rx)


async def test_process_as_connect_request_oversized():
    client_tx, proxy_rx = trio.testing.memory_stream_pair()
    huge_header = b"X-Huge: " + b"a" * 120 + b"\r\n"
    await client_tx.send_all(
        b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n"
        + huge_header
        + b"\r\n"
    )
    with pytest.raises(RequestTooLargeError):
        await process_as_connect_request(proxy_rx, maxreqsz=100)


async def test_process_as_connect_request_timeout():
    _client_tx, proxy_rx = trio.testing.memory_stream_pair()
    with pytest.raises(trio.TooSlowError):
        await process_as_connect_request(proxy_rx, read_timeout=0.01)
