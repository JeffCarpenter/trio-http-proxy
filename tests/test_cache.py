from typing import Any, Self

from trio_http_proxy.cache import (
    ProxyCache,
    get_client_key,
)
from trio_http_proxy.proxy_cache import get_default_proxy_cache


def connect_request(target: bytes = b"example.com:443", extra: bytes = b"") -> bytes:
    return b"CONNECT " + target + b" HTTP/1.1\r\nHost: example.com\r\n\r\n" + extra


class MockSocket:
    def __init__(self, ip: str = "192.168.1.50") -> None:
        self._ip = ip

    def getpeername(self) -> tuple[str, int]:
        return (self._ip, 12345)


class StreamWithSocket:
    def __init__(self, inner: Any, ip: str = "192.168.1.50") -> None:
        self._inner: Any = inner
        self.socket = MockSocket(ip)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if hasattr(self._inner, "aclose"):
            await self._inner.aclose()


def test_get_client_key():
    class DummyStreamWithSocket:
        class Socket:
            def getpeername(self):
                return ("10.0.0.1", 54321)

        socket = Socket()

    assert get_client_key(DummyStreamWithSocket()) == "10.0.0.1"
    assert get_client_key(object()) is None


def test_default_proxy_cache_singleton():
    cache = get_default_proxy_cache()
    assert isinstance(cache, ProxyCache)
    assert cache is get_default_proxy_cache()  # memoized singleton
