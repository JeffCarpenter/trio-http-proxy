import trio

from trio_http_proxy.cache import ProxyCache, get_client_key
from trio_http_proxy.proxy_cache import get_default_proxy_cache


async def test_get_client_key(dest_server_port):
    async with await trio.open_tcp_stream("127.0.0.1", dest_server_port) as stream:
        assert get_client_key(stream) == "127.0.0.1"
    assert get_client_key(object()) is None


def test_default_proxy_cache_singleton():
    cache = get_default_proxy_cache()
    assert isinstance(cache, ProxyCache)
    assert cache is get_default_proxy_cache()  # memoized singleton
