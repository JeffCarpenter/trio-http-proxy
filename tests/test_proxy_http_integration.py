from typing import Any

import pytest
import trio
import trio.testing as tt

from trio_http_proxy import http, proxy
from trio_http_proxy.cache import CacheBackendUnavailable, RateLimitError


def connect_request(target: bytes = b"example.com:443") -> bytes:
    return b"CONNECT " + target + b" HTTP/1.1\r\nHost: example.com\r\n\r\n"


class PeerSocket:
    def getpeername(self) -> tuple[str, int]:
        return ("127.0.0.1", 12345)


def attach_peer(stream: Any) -> None:
    stream.socket = PeerSocket()


@pytest.mark.parametrize("stage", ["rate", "circuit", "dns"])
@tt.trio_test
async def test_http_proxy_cache_backend_failure_sends_503(stage: str) -> None:
    proxy_stream, client_stream = tt.memory_stream_pair()
    attach_peer(proxy_stream)

    class FailingCache:
        async def check_rate_limit(self, _key: str) -> None:
            if stage == "rate":
                raise CacheBackendUnavailable("rate backend down")

        async def check_circuit(self, _host: str, _port: int) -> None:
            if stage == "circuit":
                raise CacheBackendUnavailable("circuit backend down")

        async def resolve_hosts(self, host: str) -> list[str]:
            if stage == "dns":
                raise CacheBackendUnavailable("DNS backend down")
            return [host]

    async def fake_connect(_host: str, _port: int) -> Any:
        raise AssertionError("destination must not be connected after cache failure")

    await client_stream.send_all(connect_request())
    await proxy.http_proxy(
        proxy_stream, _connect=fake_connect, proxy_cache=FailingCache()
    )

    response = await client_stream.receive_some(4096)
    assert response == http.http_503_service_unavailable
    assert b"502" not in response
    assert b"429" not in response
    assert await client_stream.receive_some(4096) == b""


@tt.trio_test
async def test_http_proxy_rate_limit_retry_after_uses_rejection_metadata() -> None:
    proxy_stream, client_stream = tt.memory_stream_pair()
    attach_peer(proxy_stream)

    class RateLimitedCache:
        async def check_rate_limit(self, _key: str) -> None:
            raise RateLimitError(retry_after=17)

    await client_stream.send_all(connect_request())
    await proxy.http_proxy(proxy_stream, proxy_cache=RateLimitedCache())

    response = await client_stream.receive_some(4096)
    assert response.startswith(b"HTTP/1.1 429 Too Many Requests\r\n")
    assert b"Retry-After: 17\r\n" in response
    assert response == proxy._http_429_response(17)
    assert await client_stream.receive_some(4096) == b""


@tt.trio_test
async def test_http_proxy_post_connect_cache_failure_closes_destination() -> None:
    proxy_stream, client_stream = tt.memory_stream_pair()
    attach_peer(proxy_stream)
    destination_stream, destination_peer = tt.memory_stream_pair()

    class FailingCache:
        async def check_rate_limit(self, _key: str) -> None:
            return None

        async def check_circuit(self, _host: str, _port: int) -> None:
            return None

        async def resolve_hosts(self, host: str) -> list[str]:
            return [host]

        async def record_connection_result(
            self, _host: str, _port: int, _error: Exception | None
        ) -> None:
            raise CacheBackendUnavailable("result backend down")

    async def fake_connect(_host: str, _port: int) -> Any:
        return destination_stream

    await client_stream.send_all(connect_request())
    await proxy.http_proxy(
        proxy_stream, _connect=fake_connect, proxy_cache=FailingCache()
    )

    response = await client_stream.receive_some(4096)
    assert response == http.http_503_service_unavailable
    assert b"200" not in response
    assert b"502" not in response
    assert b"429" not in response
    with pytest.raises(trio.ClosedResourceError):
        await destination_stream.send_all(b"must not tunnel")
    await destination_peer.aclose()
