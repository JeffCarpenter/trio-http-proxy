import errno
from functools import partial
from typing import Any, cast

import pytest
import trio
import trio.testing as tt

from trio_http_proxy import http, proxy
from trio_http_proxy.cache import ProxyCache


@pytest.fixture
def mock_clock():
    return tt.MockClock(autojump_threshold=0)


def connect_request(target=b"example.com:443", extra=b""):
    return b"CONNECT " + target + b" HTTP/1.1\r\nHost: example.com\r\n\r\n" + extra


# ---------------------------------------------------------------------------
# Invariant 1: Custom proxy_cache passed to start_server is exact object used
# ---------------------------------------------------------------------------
@tt.trio_test
async def test_invariant_1_start_server_uses_exact_custom_proxy_cache():
    """A custom proxy_cache passed to start_server is the exact object used by handlers."""

    class CustomCache:
        def __init__(self):
            self.started = False
            self.stopped = False
            self.checked_rate_limit = False

        def start(self):
            self.started = True

        def stop(self):
            self.stopped = True

        async def check_rate_limit(self, client_key):
            self.checked_rate_limit = True

    custom_cache = CustomCache()
    client_stream, test_client = tt.memory_stream_pair()

    class FakeSocket:
        def getpeername(self):
            return ("127.0.0.1", 12345)

    cast(Any, client_stream).socket = FakeSocket()

    async def fake_serve(handler, port):
        await test_client.send_all(b"GARBAGE\r\n\r\n")
        await handler(client_stream)

    await proxy.start_server(
        server=proxy.http_proxy,
        port=0,
        _serve=fake_serve,
        proxy_cache=custom_cache,
    )

    assert custom_cache.started is True
    assert custom_cache.stopped is True
    assert custom_cache.checked_rate_limit is True


@tt.trio_test
async def test_invariant_1_start_server_resets_cache_context_after_shutdown():
    """A stopped custom cache is not reused by later defaulted calls."""
    custom_cache = type(
        "CustomCache", (), {"start": lambda self: None, "stop": lambda self: None}
    )()
    checks = []

    class FallbackCache:
        async def check_rate_limit(self, client_key):
            checks.append(client_key)

    fallback_cache = FallbackCache()
    cv_token = proxy.CV_PROXY_CACHE.set(fallback_cache)

    async def fake_serve(handler, port):
        return None

    try:
        await proxy.start_server(
            server=proxy.http_proxy,
            port=0,
            _serve=fake_serve,
            proxy_cache=custom_cache,
        )

        proxy_client, test_client = tt.memory_stream_pair()

        class FakeSocket:
            def getpeername(self):
                return ("127.0.0.1", 12345)

        cast(Any, proxy_client).socket = FakeSocket()
        await test_client.send_all(b"GARBAGE\r\n\r\n")
        await proxy.http_proxy(proxy_client)

        assert checks == ["127.0.0.1"]
    finally:
        proxy.CV_PROXY_CACHE.reset(cv_token)


@tt.trio_test
async def test_invariant_1_custom_proxy_cache_passed_to_custom_handler():
    """If a custom connection handler accepts proxy_cache, start_server passes the exact object."""
    custom_cache = object()
    received_cache = None

    async def custom_handler(stream, proxy_cache=None):
        nonlocal received_cache
        received_cache = proxy_cache

    async def fake_serve(handler, port):
        client_stream, _ = tt.memory_stream_pair()
        await handler(client_stream)

    await proxy.start_server(
        server=custom_handler,
        port=0,
        _serve=fake_serve,
        proxy_cache=custom_cache,
    )

    assert received_cache is custom_cache


# ---------------------------------------------------------------------------
# Invariant 2: Gate before accept bounds accepted FDs and tasks
# ---------------------------------------------------------------------------
@tt.trio_test
async def test_invariant_2_gate_before_accept_bounds_accepted_fds():
    """MAXCONNS+N idle clients do not create unbounded accepted FDs/tasks (gate before accept)."""
    max_conns = 2
    limiter = trio.CapacityLimiter(max_conns)
    accepted_streams = []

    async def spy_handler(stream):
        accepted_streams.append(stream)
        await trio.sleep_forever()

    async with trio.open_nursery() as nursery:
        listeners = await nursery.start(
            partial(
                proxy.start_server,
                server=spy_handler,
                port=0,
                proxy_cache=None,
                limiter=limiter,
            )
        )
        listener = listeners[0]

        # Connect MAXCONNS + 3 clients
        clients = []
        for _ in range(max_conns + 3):
            c = await tt.open_stream_to_socket_listener(listener)
            clients.append(c)

        await tt.wait_all_tasks_blocked()

        # Gate before accept means exactly max_conns connections are accepted
        assert len(accepted_streams) == max_conns
        assert limiter.borrowed_tokens == max_conns

        # Cleanup
        nursery.cancel_scope.cancel()
        for c in clients:
            await c.aclose()


# ---------------------------------------------------------------------------
# Invariant 3: If client vanishes after destination connect, close dest_stream
# ---------------------------------------------------------------------------
@tt.trio_test
async def test_invariant_3_dest_stream_closed_if_client_vanishes():
    """If the client vanishes after destination connect, close destination stream."""
    proxy_client, test_client = tt.memory_stream_pair()
    dest_server, _dest_client = tt.memory_stream_pair()

    await test_client.send_all(connect_request())

    async def fake_connect(host, port):
        # Client vanishes right after destination is connected, before 200 is received
        await test_client.aclose()
        return dest_server

    await proxy.http_proxy(
        proxy_client,
        _connect=fake_connect,
        proxy_cache=None,
    )

    # Destination stream must be closed
    with pytest.raises(trio.ClosedResourceError):
        await dest_server.send_all(b"test")


# ---------------------------------------------------------------------------
# Invariant 4: CONNECT header < max with trailing bytes making total > max
# ---------------------------------------------------------------------------
@tt.trio_test
async def test_invariant_4_header_smaller_than_max_with_trailing_bytes_accepted():
    """A CONNECT header smaller than max is accepted even when the same read includes
    trailing bytes making total > max, and trailing bytes are preserved."""
    server, client = tt.memory_stream_pair()
    maxreqsz = 100
    connect_req = b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com\r\n\r\n"
    assert len(connect_req) < maxreqsz

    trailing = b"X" * 80
    assert len(connect_req) + len(trailing) > maxreqsz

    await client.send_all(connect_req + trailing)
    host, port, trailing_data = await proxy.process_as_connect_request(
        server, maxreqsz=maxreqsz
    )

    assert (host, port) == ("example.com", 443)
    assert trailing_data == trailing


# ---------------------------------------------------------------------------
# Invariant 5: Header exactly MAXREQSZ accepted; incomplete or larger rejected
# ---------------------------------------------------------------------------
@tt.trio_test
async def test_invariant_5_header_exactly_maxreqsz_accepted():
    """A complete header exactly MAXREQSZ bytes is accepted."""
    server, client = tt.memory_stream_pair()
    maxreqsz = 100
    base = b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com\r\nX: "
    pad = b"a" * (maxreqsz - len(base) - 4)
    req = base + pad + b"\r\n\r\n"
    assert len(req) == maxreqsz

    await client.send_all(req)
    host, port, trailing = await proxy.process_as_connect_request(
        server, maxreqsz=maxreqsz
    )
    assert (host, port) == ("example.com", 443)
    assert trailing == b""


@tt.trio_test
async def test_invariant_5_header_incomplete_at_maxreqsz_rejected():
    """An incomplete header reaching MAXREQSZ is rejected."""
    server, client = tt.memory_stream_pair()
    maxreqsz = 100
    incomplete = b"CONNECT example.com:443 HTTP/1.1\r\n" + b"X" * 66
    assert len(incomplete) == maxreqsz
    assert b"\r\n\r\n" not in incomplete

    await client.send_all(incomplete)
    with pytest.raises(proxy.RequestTooLargeError):
        await proxy.process_as_connect_request(server, maxreqsz=maxreqsz)


@tt.trio_test
async def test_invariant_5_header_larger_than_maxreqsz_rejected():
    """A complete header larger than MAXREQSZ is rejected."""
    server, client = tt.memory_stream_pair()
    maxreqsz = 100
    base = b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com\r\nX: "
    pad = b"a" * (maxreqsz - len(base) - 4 + 1)
    req = base + pad + b"\r\n\r\n"
    assert len(req) == maxreqsz + 1

    await client.send_all(req)
    with pytest.raises(proxy.RequestTooLargeError):
        await proxy.process_as_connect_request(server, maxreqsz=maxreqsz)


# ---------------------------------------------------------------------------
# Invariant 6: Invalid target octets yield deterministic HTTP 400
# ---------------------------------------------------------------------------
@tt.trio_test
async def test_invariant_6_invalid_target_octets_yields_400():
    """Invalid target octets yield deterministic HTTP 400."""
    proxy_client, test_client = tt.memory_stream_pair()

    # Send target with invalid target octets / invalid URL format (e.g. malformed IPv6 brackets)
    invalid_req = b"CONNECT [:443 HTTP/1.1\r\nHost: example.com\r\n\r\n"
    await test_client.send_all(invalid_req)

    await proxy.http_proxy(proxy_client, proxy_cache=None)

    response = await test_client.receive_some(4096)
    assert response == http.http_400_bad_request


@tt.trio_test
async def test_invariant_6_non_ascii_target_octet_yields_400():
    """A literal non-ASCII request-target octet yields deterministic HTTP 400."""
    proxy_client, test_client = tt.memory_stream_pair()

    await test_client.send_all(connect_request(target=b"\xff:443"))
    await proxy.http_proxy(proxy_client, proxy_cache=None)

    response = await test_client.receive_some(4096)
    assert response == http.http_400_bad_request


@tt.trio_test
async def test_invariant_6_malformed_host_octets_yields_400():
    """Target with invalid brackets or unparseable host yields deterministic HTTP 400."""
    proxy_client, test_client = tt.memory_stream_pair()

    invalid_req = b"CONNECT bad[host]:443 HTTP/1.1\r\nHost: example.com\r\n\r\n"
    await test_client.send_all(invalid_req)

    await proxy.http_proxy(proxy_client, proxy_cache=None)

    response = await test_client.receive_some(4096)
    assert response == http.http_400_bad_request


# ---------------------------------------------------------------------------
# Invariant 7: Fallback on timeout within overall bounded connect timeout
# ---------------------------------------------------------------------------
@tt.trio_test
async def test_invariant_7_fallback_when_first_address_times_out():
    """If the first cached address times out, another address is attempted."""
    proxy_client, test_client = tt.memory_stream_pair()
    dest_server, _dest_client = tt.memory_stream_pair()

    tried = []

    async def fake_connect(host, port):
        tried.append(host)
        if host == "192.0.2.1":
            raise trio.TooSlowError("Host 1 timed out")
        return dest_server

    class MultiDnsCache(ProxyCache):
        async def resolve_hosts(self, host: str):
            return ["192.0.2.1", "192.0.2.2"]

    await test_client.send_all(connect_request(target=b"dual.example.com:443"))

    pc = MultiDnsCache(cache_url="mem://", dns_cache_ttl="60s")
    stream = await proxy.tunnel(
        proxy_client,
        _connect=fake_connect,
        proxy_cache=pc,
        dest_connect_timeout=2.0,
    )
    assert stream is dest_server
    assert tried == ["192.0.2.1", "192.0.2.2"]


@tt.trio_test
async def test_invariant_7_overall_connect_timeout_bounded(mock_clock):
    """DEST_CONNECT_TIMEOUT is an overall bound across all address attempts."""
    proxy_client, test_client = tt.memory_stream_pair()

    tried = []

    async def fake_connect(host, port):
        tried.append(host)
        # Simulate hang / blackhole
        await trio.sleep_forever()

    class MultiDnsCache:
        async def resolve_hosts(self, host: str):
            return ["192.0.2.1", "192.0.2.2", "192.0.2.3"]

        async def check_circuit(self, host: str, port: int):
            pass

        async def record_connection_result(self, _record: object):
            pass

    await test_client.send_all(connect_request(target=b"multi.example.com:443"))

    pc = MultiDnsCache()
    start_time = trio.current_time()
    dest_timeout = 6.0

    with pytest.raises(trio.TooSlowError):
        await proxy.tunnel(
            proxy_client,
            _connect=fake_connect,
            proxy_cache=pc,
            dest_connect_timeout=dest_timeout,
        )

    elapsed = trio.current_time() - start_time
    assert elapsed <= dest_timeout + 0.01


@tt.trio_test
async def test_invariant_2_limiter_token_turns_over_after_handler_finishes():
    """A completed handler releases admission for the next accepted stream."""
    limiter = trio.CapacityLimiter(1)
    streams = [tt.memory_stream_pair()[0] for _ in range(2)]
    handler_finished = trio.Event()

    class FakeListener:
        def __init__(self):
            self.calls = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info):
            return None

        async def accept(self):
            if self.calls >= len(streams):
                await trio.sleep_forever()
            stream = streams[self.calls]
            self.calls += 1
            return stream

    listener = FakeListener()

    async def handler(stream):
        await trio.lowlevel.checkpoint()
        if listener.calls >= 2:
            handler_finished.set()

    async with trio.open_nursery() as nursery:
        await nursery.start(
            partial(
                proxy._serve_tcp_gated,
                handler,
                0,
                limiter=limiter,
                listeners=cast(Any, [listener]),
            )
        )
        await handler_finished.wait()
        assert listener.calls == 2
        # The loop has admitted the second stream after the first handler
        # released its token. It is now blocked in the next accept.
        assert limiter.borrowed_tokens == 1
        nursery.cancel_scope.cancel()

    assert limiter.borrowed_tokens == 0


@tt.trio_test
async def test_invariant_2_limiter_token_released_when_accept_loop_cancelled():
    """Shutdown while accept is blocked releases the reserved admission token."""
    limiter = trio.CapacityLimiter(1)
    accept_started = trio.Event()

    class FakeListener:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info):
            return None

        async def accept(self):
            accept_started.set()
            await trio.sleep_forever()

    async def handler(stream):
        raise AssertionError("no stream should be accepted")

    listener = FakeListener()
    async with trio.open_nursery() as nursery:
        await nursery.start(
            partial(
                proxy._serve_tcp_gated,
                handler,
                0,
                limiter=limiter,
                listeners=cast(Any, [listener]),
            )
        )
        await accept_started.wait()
        assert limiter.borrowed_tokens == 1
        nursery.cancel_scope.cancel()

    assert limiter.borrowed_tokens == 0


@tt.trio_test
async def test_invariant_2_failed_handler_start_closes_stream_and_releases_token():
    """A failed handler task admission closes its stream and returns capacity."""
    limiter = trio.CapacityLimiter(1)

    class FakeStream:
        closed = False

        async def aclose(self):
            self.closed = True

    class FakeListener:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info):
            return None

        async def accept(self):
            return stream

    class FailingNursery:
        def start_soon(self, *args):
            raise RuntimeError("nursery is shutting down")

    stream = FakeStream()
    with pytest.raises(RuntimeError, match="nursery is shutting down"):
        await proxy._serve_one_listener_gated(
            cast(Any, FakeListener()),
            cast(Any, FailingNursery()),
            lambda _stream: None,
            limiter,
        )

    assert stream.closed is True
    assert limiter.borrowed_tokens == 0


@pytest.mark.parametrize(
    "accept_errno", [errno.EMFILE, errno.ENFILE, errno.ENOBUFS, errno.ENOMEM]
)
@tt.trio_test
async def test_invariant_2_transient_accept_errors_retry(
    accept_errno, caplog, mock_clock
):
    """Transient accept resource errors release capacity, sleep, and retry."""
    limiter = trio.CapacityLimiter(1)
    stream = tt.memory_stream_pair()[0]
    accepted = trio.Event()

    class FakeListener:
        def __init__(self):
            self.calls = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info):
            return None

        async def accept(self):
            self.calls += 1
            if self.calls == 1:
                raise OSError(accept_errno, "temporary resource exhaustion")
            return stream

    listener = FakeListener()

    async def handler(_stream):
        accepted.set()
        await trio.sleep_forever()

    async with trio.open_nursery() as nursery:
        await nursery.start(
            partial(
                proxy._serve_tcp_gated,
                handler,
                0,
                limiter=limiter,
                listeners=cast(Any, [listener]),
            )
        )
        await accepted.wait()
        assert listener.calls == 2
        assert limiter.borrowed_tokens == 1
        nursery.cancel_scope.cancel()

    assert limiter.borrowed_tokens == 0
    assert f"accept returned {errno.errorcode[accept_errno]}" in caplog.text
