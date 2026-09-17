import logging
import subprocess
import sys

import pytest
import trio
import trio.testing as tt

from trio_http_proxy import http, proxy
from trio_http_proxy.cache import CircuitBreakerOpen, ProxyCache


def connect_request(target=b"example.com:443", extra=b""):
    return b"CONNECT " + target + b" HTTP/1.1\r\nHost: example.com\r\n\r\n" + extra


# ---------------------------------------------------------------------------
# C1: Pipelined post-CONNECT bytes dropped
# ---------------------------------------------------------------------------
@tt.trio_test
async def test_repro_c1_pipelined_early_tls_forwarded():
    """Early bytes following CONNECT headers must be forwarded to dest."""
    proxy_client, test_client = tt.memory_stream_pair()
    dest_server, dest_client = tt.memory_stream_pair()

    early_data = b"\x16\x03\x01EARLYTLS"
    await test_client.send_all(connect_request(extra=early_data))

    async def fake_connect(host, port):
        return dest_server

    dest_stream = await proxy.tunnel(
        proxy_client, _connect=fake_connect, proxy_cache=None
    )
    assert dest_stream is dest_server

    # Dest should receive the early data immediately upon tunnel establishment
    with trio.fail_after(0.2):
        received = await dest_client.receive_some(4096)
    assert received == early_data


# ---------------------------------------------------------------------------
# C2: Exceptions escaping http_proxy
# ---------------------------------------------------------------------------
@tt.trio_test
async def test_repro_c2_broken_resource_does_not_escape_http_proxy():
    """BrokenResourceError when client closes early must not escape http_proxy."""
    proxy_client, test_client = tt.memory_stream_pair()
    await test_client.send_all(b"GARBAGE NOT CONNECT\r\n\r\n")
    # Close test_client so proxy_client.send_all(400) raises BrokenResourceError
    await test_client.aclose()

    # Must complete cleanly without raising BrokenResourceError to caller
    await proxy.http_proxy(proxy_client, proxy_cache=None)


@tt.trio_test
async def test_repro_c2_rate_limit_broken_resource_does_not_escape():
    """If client stream is broken during 429 response, http_proxy must not raise."""
    proxy_client, test_client = tt.memory_stream_pair()

    pc = ProxyCache(rate_limit_enabled=True, rate_limit=1)
    await pc.check_rate_limit("127.0.0.1")  # exhaust rate limit

    # Mock get_client_key to return an IP
    await test_client.aclose()
    # http_proxy should catch BrokenResourceError or any exception sending 429
    await proxy.http_proxy(proxy_client, proxy_cache=pc)
    pc.stop()


# ---------------------------------------------------------------------------
# C3: Circuit breaker threshold effectively doubled
# ---------------------------------------------------------------------------
@tt.trio_test
async def test_repro_c3_circuit_breaker_errors_rate_80_trips():
    """Circuit breaker with errors_rate=80 must trip when 100% of requests fail."""
    async with ProxyCache(
        cache_url="mem://",
        circuit_breaker_enabled=True,
        circuit_breaker_errors_rate=80,
        circuit_breaker_min_calls=2,
    ) as pc:
        host, port = "unstable.example.com", 443

        # Request 1 fails
        await pc.check_circuit(host, port)
        await pc.record_connection_result(host, port, OSError("conn refused"))

        # Request 2 fails
        await pc.check_circuit(host, port)
        await pc.record_connection_result(host, port, OSError("conn refused"))

        # Next check_circuit MUST raise CircuitBreakerOpen (2 fails out of 2 = 100% >= 80%)
        with pytest.raises(CircuitBreakerOpen):
            await pc.check_circuit(host, port)


# ---------------------------------------------------------------------------
# C4: 504 is never sent when circuit breaker enabled
# ---------------------------------------------------------------------------
@tt.trio_test
async def test_repro_c4_504_sent_when_circuit_breaker_enabled():
    """Destination connect timeout must reply 504 even with circuit breaker enabled."""
    proxy_client, test_client = tt.memory_stream_pair()

    async with ProxyCache(
        cache_url="mem://",
        circuit_breaker_enabled=True,
    ) as pc:

        async def slow_connect(host, port):
            await trio.sleep_forever()

        await test_client.send_all(connect_request(target=b"timeout.example.com:443"))

        with pytest.raises(trio.TooSlowError):
            await proxy.tunnel(
                proxy_client,
                _connect=slow_connect,
                dest_connect_timeout=0.05,
                proxy_cache=pc,
            )

        with trio.fail_after(0.2):
            reply = await test_client.receive_some(4096)
        assert b"504 Gateway Timeout" in reply


# ---------------------------------------------------------------------------
# C5: KeyboardInterrupt in start_server
# ---------------------------------------------------------------------------
@tt.trio_test
async def test_repro_c5_keyboard_interrupt_caught_in_start_server():
    """BaseExceptionGroup wrapping KeyboardInterrupt must be caught by start_server."""

    async def fake_serve(handler, port):
        raise BaseExceptionGroup("serve", [KeyboardInterrupt()])

    # Should catch KeyboardInterrupt and return normally without propagating
    await proxy.start_server(
        server=proxy.http_proxy,
        port=0,
        _serve=fake_serve,
        proxy_cache=None,
    )


# ---------------------------------------------------------------------------
# C6: Documented run commands
# ---------------------------------------------------------------------------
def test_repro_c6_main_module_exists():
    """`python -m trio_http_proxy` must be executable."""
    proc = subprocess.run(
        [sys.executable, "-m", "trio_http_proxy", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    # Even if it errors on invalid args or starts server, it must NOT fail with:
    # "No module named trio_http_proxy.__main__"
    assert "No module named trio_http_proxy.__main__" not in proc.stderr


# ---------------------------------------------------------------------------
# R1: Half-close in pipe
# ---------------------------------------------------------------------------
@tt.trio_test
async def test_repro_r1_pipe_calls_send_eof_on_from_stream_eof():
    """pipe() must invoke to_stream.send_eof() when from_stream reaches EOF."""
    from_server, from_client = tt.memory_stream_pair()
    to_server, to_client = tt.memory_stream_pair()

    async with trio.open_nursery() as nursery:
        nursery.start_soon(proxy.pipe, from_server, to_server)
        await from_client.send_all(b"hello")
        await from_client.send_eof()

    # After pipe finishes, to_client should see EOF
    chunk = await to_client.receive_some(4096)
    assert chunk == b"hello"
    with trio.fail_after(0.1):
        eof_chunk = await to_client.receive_some(4096)
        assert eof_chunk == b""


# ---------------------------------------------------------------------------
# R3: stop() prevents auto-restart
# ---------------------------------------------------------------------------
@tt.trio_test
async def test_repro_r3_stop_prevents_auto_restart():
    """Calling stop() must prevent subsequent operations from silently restarting the thread."""
    pc = ProxyCache(cache_url="mem://")
    pc.start()
    assert pc._started is True
    pc.stop()
    assert pc._started is False

    # Calling run_coroutine after stop must raise RuntimeError, not restart
    async def dummy():
        return 42

    with pytest.raises(RuntimeError):
        pc.run_coroutine(dummy())
    assert pc._started is False


# ---------------------------------------------------------------------------
# R4: DNS cache fallback across multiple IPs
# ---------------------------------------------------------------------------
@tt.trio_test
async def test_repro_r4_dns_cache_fallback():
    """When a host resolves to multiple IPs and the first fails, tunnel falls back to second."""
    proxy_client, test_client = tt.memory_stream_pair()
    dest_server, _dest_client = tt.memory_stream_pair()

    tried = []

    async def fake_connect(host, port):
        tried.append(host)
        if host == "192.0.2.1":
            raise OSError("Connection refused on IPv4 #1")
        return dest_server

    class MultiDnsCache(ProxyCache):
        async def resolve_hosts(self, host: str):
            return ["192.0.2.1", "192.0.2.2"]

    await test_client.send_all(connect_request(target=b"dual.example.com:443"))

    pc = MultiDnsCache(cache_url="mem://", dns_cache_ttl="60s")
    stream = await proxy.tunnel(proxy_client, _connect=fake_connect, proxy_cache=pc)
    assert stream is dest_server
    assert tried == ["192.0.2.1", "192.0.2.2"]


# ---------------------------------------------------------------------------
# R5: No _is_mock_clock() in cache.py
# ---------------------------------------------------------------------------
def test_repro_r5_no_mock_clock_in_cache():
    """_is_mock_clock() should not exist on ProxyCache."""
    pc = ProxyCache()
    assert not hasattr(pc, "_is_mock_clock")


# ---------------------------------------------------------------------------
# Tier 3 Nits
# ---------------------------------------------------------------------------
def test_repro_tier3_bufmaxsz_zero_invalid():
    """BUFMAXSZ=0 must fail validation with ValueError."""
    with pytest.raises(ValueError, match="BUFMAXSZ must be >= 1"):
        proxy._validate_bufmaxsz(0)


def test_repro_tier3_error_responses_headers():
    """Error responses must contain Connection: close and Content-Length: 0."""
    for resp in (
        http.http_400_bad_request,
        http.http_408_request_timeout,
        http.http_414_uri_too_long,
        http.http_429_too_many_requests,
        http.http_502_bad_gateway,
        http.http_503_service_unavailable,
        http.http_504_gateway_timeout,
    ):
        assert b"Connection: close\r\n" in resp
        assert b"Content-Length: 0\r\n" in resp


@tt.trio_test
async def test_repro_tier3_cv_desthost_set_on_error_path():
    """CV_DESTHOST must be set even when destination connection fails."""
    proxy_client, test_client = tt.memory_stream_pair()

    async def failing_connect(host, port):
        # Verify CV_DESTHOST was set before connect
        assert proxy.CV_DESTHOST.get() == "failing.example.com"
        raise OSError("connection refused")

    await test_client.send_all(connect_request(target=b"failing.example.com:443"))

    with pytest.raises(OSError):
        await proxy.tunnel(proxy_client, _connect=failing_connect, proxy_cache=None)

    assert proxy.CV_DESTHOST.get() == "failing.example.com"


@tt.trio_test
async def test_repro_tier3_dns_and_connect_errors_log_as_warning_not_exception(caplog):
    """DNS misses and connection errors should log warning without traceback."""
    proxy_client, test_client = tt.memory_stream_pair()

    async def failing_connect(host, port):
        raise OSError("connection refused")

    await test_client.send_all(connect_request(target=b"failing.example.com:443"))

    with pytest.raises(OSError):
        await proxy.tunnel(proxy_client, _connect=failing_connect, proxy_cache=None)

    # Find the log record
    connect_records = [r for r in caplog.records if "Could not connect" in r.message]
    assert len(connect_records) == 1
    assert connect_records[0].levelno == logging.WARNING
    assert connect_records[0].exc_info is None or connect_records[0].exc_info is False
