from trio_http_proxy.cache import ProxyCache


def test_bridge_thread_spawned_when_dns_cache_feature_enabled_control() -> None:
    """Positive control: bridge thread MUST be spawned when DNS caching feature is enabled."""
    cache = ProxyCache(
        enabled=True,
        cache_url="mem://",
        dns_cache_ttl="10s",
        rate_limit_enabled=False,
    )
    cache.start()

    try:
        assert cache._thread is not None, (
            "Expected bridge thread to be spawned when dns_cache_ttl > 0s"
        )
        assert cache._thread.is_alive() is True, (
            "Bridge thread should be active and running"
        )
        assert cache._loop is not None, "Expected asyncio event loop to be initialized"
        assert cache.is_running is True
    finally:
        cache.stop()

    assert cache.is_stopped is True
    assert cache.is_running is False
    assert cache._thread is None
    assert cache._loop is None


def test_bridge_thread_spawned_when_rate_limit_feature_enabled_control() -> None:
    """Positive control: bridge thread MUST be spawned when rate limiting feature is enabled."""
    cache = ProxyCache(
        enabled=True,
        cache_url="mem://",
        dns_cache_ttl="0s",
        rate_limit_enabled=True,
        rate_limit=10,
    )
    cache.start()

    try:
        assert cache._thread is not None, (
            "Expected bridge thread to be spawned when rate_limit_enabled is True"
        )
        assert cache._thread.is_alive() is True, (
            "Bridge thread should be active and running"
        )
        assert cache._loop is not None, "Expected asyncio event loop to be initialized"
        assert cache.is_running is True
    finally:
        cache.stop()

    assert cache.is_stopped is True
    assert cache.is_running is False
    assert cache._thread is None
    assert cache._loop is None
