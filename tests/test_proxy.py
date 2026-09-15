import contextvars
import logging
import random
from functools import partial
from typing import cast

import h11
import pytest
import trio
import trio.testing as tt

from trio_http_proxy import proxy


def run_isolated(fn, *args, **kwargs):
    """Run fn(*args, **kwargs) inside a fresh copy of the current context so
    any ContextVar.set() calls inside it cannot leak into later tests."""
    return contextvars.copy_context().run(fn, *args, **kwargs)


def connect_request(target=b"example.com:443", extra=b""):
    return b"CONNECT " + target + b" HTTP/1.1\r\nHost: example.com\r\n\r\n" + extra


def fake_stream():
    """A sentinel that satisfies the CV_*_STREAM ContextVar's SocketStream
    type for log() tests, which only compare stream identity."""
    return cast(trio.SocketStream, object())


@pytest.fixture
def mock_clock():
    """Passed by name to a @tt.trio_test function, trio_test's wrapper
    detects any Clock-typed fixture in its kwargs and hands it to
    trio.run(clock=...). autojump_threshold=0 makes the run jump straight
    to the next scheduled deadline whenever every task is blocked, so a
    slowloris-style test with a multi-second (mock) timeout resolves in
    near-zero real wall-clock time."""
    return tt.MockClock(autojump_threshold=0)


@pytest.fixture(autouse=True)
def _enable_log_capture(caplog):
    caplog.set_level(logging.DEBUG)


# ---------------------------------------------------------------------------
# process_as_connect_request
# ---------------------------------------------------------------------------


@tt.trio_test
async def test_process_as_connect_request_success():
    server, client = tt.memory_stream_pair()
    await client.send_all(connect_request())
    host, port = await proxy.process_as_connect_request(server)
    assert (host, port) == ("example.com", 443)


@tt.trio_test
async def test_process_as_connect_request_chunked_across_multiple_reads():
    # memory_stream_pair() buffers unboundedly, so sending the whole request
    # before the first read would let one receive_some() slurp it all in one
    # shot. Trickle it in, yielding between chunks, to force process_as_
    # connect_request's NEED_DATA loop to actually run more than once.
    server, client = tt.memory_stream_pair()
    req = connect_request()
    read_count = 0

    async def count_reads():
        nonlocal read_count
        read_count += 1

    server.receive_stream.receive_some_hook = count_reads

    async def trickle():
        for i in range(0, len(req), 3):
            await client.send_all(req[i : i + 3])
            await tt.wait_all_tasks_blocked()

    async with trio.open_nursery() as nursery:
        nursery.start_soon(trickle)
        host, port = await proxy.process_as_connect_request(server)

    assert (host, port) == ("example.com", 443)
    # Confirms the trickle actually forced multiple receive_some() calls,
    # rather than the whole request landing in one buffered read.
    assert read_count > 1


@tt.trio_test
async def test_process_as_connect_request_rejects_non_connect_method():
    server, client = tt.memory_stream_pair()
    await client.send_all(b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
    with pytest.raises(proxy.ProxyProtocolError, match="CONNECT"):
        await proxy.process_as_connect_request(server)


@tt.trio_test
async def test_process_as_connect_request_rejects_malformed_http():
    server, client = tt.memory_stream_pair()
    await client.send_all(b"this is not http at all\r\n\r\n")
    with pytest.raises(h11.ProtocolError):
        await proxy.process_as_connect_request(server)


@tt.trio_test
async def test_process_as_connect_request_enforces_max_size():
    server, client = tt.memory_stream_pair()
    huge_target = b"a" * 100 + b":443"
    await client.send_all(connect_request(target=huge_target))
    with pytest.raises(proxy.RequestTooLargeError, match="did not fit"):
        await proxy.process_as_connect_request(server, bufmaxsz=8, maxreqsz=32)


@tt.trio_test
async def test_process_as_connect_request_honors_maxreqsz_above_h11_default():
    # h11.Connection's own max_incomplete_event_size defaults to 16384 bytes,
    # independent of maxreqsz, unless we pass it through explicitly. Without
    # that wiring, a maxreqsz configured above 16384 would never actually
    # apply: h11 raises its own RemoteProtocolError once the buffered-but-
    # still-incomplete request exceeds 16384 bytes, before our own maxreqsz
    # guard ever gets a chance to fire.
    #
    # h11 only enforces that ceiling while a request is still incomplete --
    # if a read happens to deliver the rest of the request in the same
    # receive_data() call that crosses 16384, it never trips. So bufmaxsz is
    # chosen to force the first receive_some() to return more than 16384
    # bytes while deliberately withholding the final CRLFCRLF (still
    # incomplete), which is what actually exercises h11's ceiling.
    server, client = tt.memory_stream_pair()
    long_host = "a" * 17000
    req = connect_request(target=f"{long_host}:443".encode())
    assert len(req) > 16384 + 100
    await client.send_all(req)
    first_read_size = 16384 + 100  # > h11's default ceiling, still incomplete
    host, port = await proxy.process_as_connect_request(
        server, bufmaxsz=first_read_size, maxreqsz=20000
    )
    assert (host, port) == (long_host, 443)


@tt.trio_test
async def test_process_as_connect_request_rejects_non_numeric_port():
    server, client = tt.memory_stream_pair()
    await client.send_all(connect_request(target=b"example.com:notaport"))
    with pytest.raises(ValueError):
        await proxy.process_as_connect_request(server)


@tt.trio_test
async def test_process_as_connect_request_rejects_missing_port():
    server, client = tt.memory_stream_pair()
    await client.send_all(connect_request(target=b"example.com"))
    with pytest.raises(ValueError):
        await proxy.process_as_connect_request(server)


@tt.trio_test
async def test_process_as_connect_request_rejects_eof_before_complete_request():
    server, client = tt.memory_stream_pair()
    await client.aclose()
    with pytest.raises(proxy.ProxyProtocolError, match="is not a h11.Request"):
        await proxy.process_as_connect_request(server)


@tt.trio_test
async def test_process_as_connect_request_rejects_partial_then_eof():
    server, client = tt.memory_stream_pair()
    await client.send_all(b"CONNECT example.com:443 HTTP/1.1\r\n")
    await client.aclose()
    # h11 treats an EOF mid-headers as a protocol violation, not a clean
    # "no request at all" close (see test_..._eof_before_complete_request).
    with pytest.raises(h11.ProtocolError, match="unexpectedly closed"):
        await proxy.process_as_connect_request(server)


@tt.trio_test
async def test_process_as_connect_request_ignores_trailing_bytes_after_headers():
    server, client = tt.memory_stream_pair()
    await client.send_all(connect_request(extra=b"leftover garbage"))
    host, port = await proxy.process_as_connect_request(server)
    assert (host, port) == ("example.com", 443)


@tt.trio_test
async def test_process_as_connect_request_supports_bracketed_ipv6_target():
    # Regression test for the naive `target.partition(b":")` parse: a
    # bracketed IPv6 authority like "[::1]:443" has colons *inside* the host
    # itself, so splitting on the first colon tears the address apart.
    server, client = tt.memory_stream_pair()
    await client.send_all(connect_request(target=b"[::1]:443"))
    host, port = await proxy.process_as_connect_request(server)
    assert (host, port) == ("::1", 443)


@tt.trio_test
async def test_process_as_connect_request_supports_full_ipv6_target():
    server, client = tt.memory_stream_pair()
    await client.send_all(connect_request(target=b"[2001:db8::1]:8443"))
    host, port = await proxy.process_as_connect_request(server)
    assert (host, port) == ("2001:db8::1", 8443)


@tt.trio_test
async def test_process_as_connect_request_rejects_target_with_path():
    # RFC 7231 4.3.6: a CONNECT request-target must be authority-form
    # (host:port) only -- no path, query, or fragment. urlsplit() is lax
    # about this and silently discards a trailing path, so a smuggled path
    # is currently accepted rather than rejected.
    server, client = tt.memory_stream_pair()
    await client.send_all(connect_request(target=b"evil.com:443/smuggled"))
    with pytest.raises(ValueError, match="authority-form"):
        await proxy.process_as_connect_request(server)


@tt.trio_test
async def test_process_as_connect_request_rejects_target_with_userinfo():
    # Same authority-form requirement: no "user:pass@" prefix either.
    server, client = tt.memory_stream_pair()
    await client.send_all(connect_request(target=b"user:pass@evil.com:443"))
    with pytest.raises(ValueError, match="authority-form"):
        await proxy.process_as_connect_request(server)


@tt.trio_test
async def test_process_as_connect_request_times_out_on_slowloris_trickle(mock_clock):
    # A slowloris client dribbles one byte every 2 (mock) seconds -- slower
    # than read_timeout=5 lets a complete request arrive. The 60s watchdog
    # is itself mock time: if proxy.py has no timeout of its own, the only
    # deadline in the run is the watchdog's, so autojump jumps straight to
    # it and cancels the wait -- a Cancelled that doesn't match
    # pytest.raises(trio.TooSlowError) propagates out to be absorbed by the
    # watchdog scope, flipping cancelled_caught. That's what proves this
    # runs in near-zero real time rather than hanging.
    server, client = tt.memory_stream_pair()

    async def slowloris():
        req = connect_request()
        for i in range(len(req)):
            await client.send_all(req[i : i + 1])
            await trio.sleep(2)

    with trio.move_on_after(60) as watchdog:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(slowloris)
            with pytest.raises(trio.TooSlowError):
                await proxy.process_as_connect_request(server, read_timeout=5)
            nursery.cancel_scope.cancel()
    assert not watchdog.cancelled_caught, "proxy never gave up on the slow client"


# ---------------------------------------------------------------------------
# tunnel
# ---------------------------------------------------------------------------


@tt.trio_test
async def test_tunnel_success_connects_and_replies_200():
    proxy_client, test_client = tt.memory_stream_pair()
    dest_proxy_side, _ = tt.memory_stream_pair()
    calls = []

    async def fake_open_tcp_stream(host, port):
        calls.append((host, port))
        return dest_proxy_side

    await test_client.send_all(connect_request(target=b"example.com:443"))
    dest_stream = await proxy.tunnel(proxy_client, _connect=fake_open_tcp_stream)

    assert calls == [("example.com", 443)]
    assert dest_stream is dest_proxy_side
    reply = await test_client.receive_some(4096)
    assert reply == b"HTTP/1.1 200 Connection established\r\n\r\n"


@tt.trio_test
async def test_tunnel_bad_request_sends_400_and_raises_without_connecting():
    proxy_client, test_client = tt.memory_stream_pair()
    called = False

    async def fake_open_tcp_stream(_host, _port):
        nonlocal called
        called = True
        raise AssertionError("should not be called")

    await test_client.send_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
    with pytest.raises(proxy.ProxyProtocolError):
        await proxy.tunnel(proxy_client, _connect=fake_open_tcp_stream)

    assert called is False
    reply = await test_client.receive_some(4096)
    assert reply == b"HTTP/1.1 400 Bad Request\r\n\r\n"


@tt.trio_test
async def test_tunnel_connect_failure_sends_502_and_raises():
    proxy_client, test_client = tt.memory_stream_pair()

    async def fake_open_tcp_stream(_host, _port):
        raise OSError("connection refused")

    await test_client.send_all(connect_request(target=b"example.com:443"))
    with pytest.raises(OSError):
        await proxy.tunnel(proxy_client, _connect=fake_open_tcp_stream)

    reply = await test_client.receive_some(4096)
    assert reply == b"HTTP/1.1 502 Bad Gateway\r\n\r\n"


@tt.trio_test
async def test_tunnel_times_out_reading_connect_request_sends_408(mock_clock):
    # Client opens the TCP connection but never sends a single byte.
    proxy_client, test_client = tt.memory_stream_pair()
    called = False

    async def fake_open_tcp_stream(_host, _port):
        nonlocal called
        called = True

    with trio.move_on_after(60) as watchdog:
        with pytest.raises(trio.TooSlowError):
            await proxy.tunnel(
                proxy_client, _connect=fake_open_tcp_stream, read_timeout=5
            )
    assert not watchdog.cancelled_caught, "proxy never gave up on the slow client"

    assert called is False
    reply = await test_client.receive_some(4096)
    assert reply == b"HTTP/1.1 408 Request Timeout\r\n\r\n"


@tt.trio_test
async def test_tunnel_times_out_connecting_to_dest_sends_504(mock_clock):
    proxy_client, test_client = tt.memory_stream_pair()

    async def hanging_connect(_host, _port):
        await trio.sleep_forever()

    await test_client.send_all(connect_request(target=b"example.com:443"))
    with trio.move_on_after(60) as watchdog:
        with pytest.raises(trio.TooSlowError):
            await proxy.tunnel(
                proxy_client, _connect=hanging_connect, dest_connect_timeout=5
            )
    assert not watchdog.cancelled_caught, "proxy never gave up on the slow destination"

    reply = await test_client.receive_some(4096)
    assert reply == b"HTTP/1.1 504 Gateway Timeout\r\n\r\n"


@tt.trio_test
async def test_tunnel_rejects_non_numeric_port_sends_400():
    # Regression test: process_as_connect_request's non-numeric-port case
    # (a bare ValueError from urlsplit(...).port, not an explicit raise) must
    # still land in tunnel()'s ProxyProtocolError/h11.ProtocolError handler
    # and get a 400 reply -- not escape unhandled and drop the client silently.
    proxy_client, test_client = tt.memory_stream_pair()

    async def fake_open_tcp_stream(_host, _port):
        raise AssertionError("should not be called")

    await test_client.send_all(connect_request(target=b"example.com:notaport"))
    with pytest.raises(proxy.ProxyProtocolError):
        await proxy.tunnel(proxy_client, _connect=fake_open_tcp_stream)

    reply = await test_client.receive_some(4096)
    assert reply == b"HTTP/1.1 400 Bad Request\r\n\r\n"


@tt.trio_test
async def test_tunnel_oversized_request_sends_414_not_bare_400():
    # process_as_connect_request's size-cap check (proxy.py:207) is a plain
    # `assert`, caught by the same except clause as a garden-variety
    # malformed request -- so an oversized CONNECT currently gets an
    # indistinguishable 400 Bad Request instead of a status that actually
    # tells the client its request was too big.
    proxy_client, test_client = tt.memory_stream_pair()
    oversized_target = b"a" * (proxy.DEFAULT_MAXREQSZ + 1) + b":443"
    await test_client.send_all(connect_request(target=oversized_target))

    async def fake_open_tcp_stream(_host, _port):
        raise AssertionError("should not be called -- request is too large")

    with pytest.raises(proxy.RequestTooLargeError):
        await proxy.tunnel(proxy_client, _connect=fake_open_tcp_stream)

    reply = await test_client.receive_some(4096)
    assert reply == b"HTTP/1.1 414 URI Too Long\r\n\r\n"


# ---------------------------------------------------------------------------
# pipe
# ---------------------------------------------------------------------------


@tt.trio_test
async def test_pipe_forwards_multiple_chunks_in_order(caplog):
    caplog.set_level(logging.DEBUG)
    from_proxy, from_test = tt.memory_stream_pair()
    to_proxy, to_test = tt.memory_stream_pair()

    async def feed():
        await from_test.send_all(b"first")
        await trio.testing.wait_all_tasks_blocked()
        await from_test.send_all(b"second")
        await trio.testing.wait_all_tasks_blocked()
        await from_test.aclose()

    async with trio.open_nursery() as nursery:
        nursery.start_soon(proxy.pipe, from_proxy, to_proxy)
        nursery.start_soon(feed)

    # pipe() only reads until EOF; it never closes to_stream itself, so we
    # close our end here to let the read loop below terminate on EOF.
    await to_proxy.aclose()
    received = b""
    while chunk := await to_test.receive_some(4096):
        received += chunk
    assert received == b"firstsecond"

    # The concatenated bytes alone can't tell "first"+"second" apart from a
    # single coalesced "firstsecond" chunk, so also check pipe()'s per-chunk
    # log lines to confirm the two sends really were forwarded separately.
    assert "Forwarded 5 bytes" in caplog.text
    assert "Forwarded 6 bytes" in caplog.text
    assert caplog.text.find("Forwarded 5 bytes") < caplog.text.find("Forwarded 6 bytes")


@tt.trio_test
async def test_pipe_immediate_eof_forwards_nothing():
    from_proxy, from_test = tt.memory_stream_pair()
    to_proxy, to_test = tt.memory_stream_pair()
    await from_test.aclose()

    await proxy.pipe(from_proxy, to_proxy)

    await to_proxy.aclose()
    remainder = await to_test.receive_some(4096)
    assert remainder == b""


@tt.trio_test
async def test_pipe_with_timeout_raises_after_idle_timeout_with_no_data(mock_clock):
    # Neither side ever sends anything or closes -- receive_some() blocks
    # forever. With no timeout of its own, pipe() would hang here (the only
    # deadline available for autojump to target would be our watchdog's), so
    # this exercises pipe_with_timeout()'s CancelScope wrapper rather than
    # pipe() itself, which no longer knows what a timeout is.
    from_proxy, _from_test = tt.memory_stream_pair()
    to_proxy, _to_test = tt.memory_stream_pair()

    with trio.move_on_after(60) as watchdog:
        with pytest.raises(trio.TooSlowError):
            await proxy.pipe_with_timeout(from_proxy, to_proxy, idle_timeout=5)
    assert not watchdog.cancelled_caught, (
        "pipe_with_timeout() never gave up on the idle peer"
    )


@tt.trio_test
async def test_pipe_with_timeout_raises_after_send_timeout_when_dest_stalls(
    mock_clock,
):
    # from_stream has data ready; to_stream never accepts it -- send_all_hook
    # fires *after* send_all() has already buffered the bytes (see
    # MemorySendStream docs), so a hook that never returns simulates a real
    # filled TCP send buffer stalling the call itself, not data never
    # arriving. With no send-side timeout, pipe() would hang here.
    from_proxy, from_test = tt.memory_stream_pair()
    to_proxy, _to_test = tt.memory_stream_pair()
    to_proxy.send_stream.send_all_hook = trio.sleep_forever

    await from_test.send_all(b"stuck payload")

    with trio.move_on_after(60) as watchdog:
        with pytest.raises(trio.TooSlowError):
            await proxy.pipe_with_timeout(
                from_proxy, to_proxy, idle_timeout=5, send_timeout=5
            )
    assert not watchdog.cancelled_caught, (
        "pipe_with_timeout() never gave up on the stalled destination"
    )


@tt.trio_test
async def test_pipe_calls_on_receive_and_on_send_hooks_per_chunk():
    from_proxy, from_test = tt.memory_stream_pair()
    to_proxy, to_test = tt.memory_stream_pair()
    receives = 0
    sends = 0

    def on_receive():
        nonlocal receives
        receives += 1

    def on_send():
        nonlocal sends
        sends += 1

    async def feed():
        await from_test.send_all(b"first")
        await trio.testing.wait_all_tasks_blocked()
        await from_test.send_all(b"second")
        await trio.testing.wait_all_tasks_blocked()
        await from_test.aclose()

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            partial(
                proxy.pipe, from_proxy, to_proxy, on_receive=on_receive, on_send=on_send
            )
        )
        nursery.start_soon(feed)

    await to_proxy.aclose()
    while await to_test.receive_some(4096):
        pass

    # 3 receives: "first", "second", then the EOF read -- EOF doesn't
    # trigger a send, so on_send() only fires for the two real chunks.
    assert receives == 3
    assert sends == 2


@tt.trio_test
async def test_http_proxy_forwards_large_payload_across_chunk_boundaries():
    # Known coverage gap (.planning/known-shortcomings/findings.md):
    # test_http_proxy_full_round_trip only sends small, single-shot payloads,
    # so pipe()'s `while True: receive_some(bufmaxsz)` loop never actually
    # iterates more than once. Send a payload several times BUFMAXSZ, with a
    # non-boundary-aligned remainder, through both directions of a real
    # tunnel, and confirm both byte-for-byte integrity across chunk
    # boundaries and that pipe() really looped -- rather than one call
    # slurping the whole (unboundedly buffered) memory stream at once.
    proxy_client, test_client = tt.memory_stream_pair()
    dest_proxy_side, dest_test_side = tt.memory_stream_pair()

    async def fake_open_tcp_stream(_host, _port):
        return dest_proxy_side

    client_to_dest_reads = 0
    dest_to_client_reads = 0

    async def count_client_to_dest_reads():
        nonlocal client_to_dest_reads
        client_to_dest_reads += 1

    async def count_dest_to_client_reads():
        nonlocal dest_to_client_reads
        dest_to_client_reads += 1

    proxy_client.receive_stream.receive_some_hook = count_client_to_dest_reads
    dest_proxy_side.receive_stream.receive_some_hook = count_dest_to_client_reads

    payload_size = proxy.BUFMAXSZ * 3 + 777
    client_payload = random.Random(1).randbytes(payload_size)
    dest_payload = random.Random(2).randbytes(payload_size)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            partial(proxy.http_proxy, proxy_client, _connect=fake_open_tcp_stream)
        )

        await test_client.send_all(connect_request(target=b"example.com:443"))
        reply = await test_client.receive_some(4096)
        assert reply == b"HTTP/1.1 200 Connection established\r\n\r\n"

        # The CONNECT handshake read above already consumed one hook call on
        # proxy_client; reset so the counter only reflects pipe()'s reads.
        client_to_dest_reads = 0

        await test_client.send_all(client_payload)
        await dest_test_side.send_all(dest_payload)

        received_by_dest = b""
        while len(received_by_dest) < payload_size:
            received_by_dest += await dest_test_side.receive_some(65536)

        received_by_client = b""
        while len(received_by_client) < payload_size:
            received_by_client += await test_client.receive_some(65536)

        assert received_by_dest == client_payload
        assert received_by_client == dest_payload

        # payload_size spans 4 bufmaxsz-sized reads (3 full + 1 partial
        # remainder); >=4 proves pipe() actually looped rather than one
        # buffered receive_some() returning everything in a single call.
        assert client_to_dest_reads >= 4
        assert dest_to_client_reads >= 4

        await test_client.aclose()
        await dest_test_side.aclose()


# ---------------------------------------------------------------------------
# http_proxy (end-to-end)
# ---------------------------------------------------------------------------


@tt.trio_test
async def test_http_proxy_full_round_trip():
    proxy_client, test_client = tt.memory_stream_pair()
    dest_proxy_side, dest_test_side = tt.memory_stream_pair()

    async def fake_open_tcp_stream(_host, _port):
        return dest_proxy_side

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            partial(proxy.http_proxy, proxy_client, _connect=fake_open_tcp_stream)
        )

        await test_client.send_all(connect_request(target=b"example.com:443"))
        reply = await test_client.receive_some(4096)
        assert reply == b"HTTP/1.1 200 Connection established\r\n\r\n"

        await test_client.send_all(b"client->dest payload")
        forwarded = await dest_test_side.receive_some(4096)
        assert forwarded == b"client->dest payload"

        await dest_test_side.send_all(b"dest->client payload")
        forwarded_back = await test_client.receive_some(4096)
        assert forwarded_back == b"dest->client payload"

        await test_client.aclose()
        await dest_test_side.aclose()


@tt.trio_test
async def test_http_proxy_closes_tunnel_after_both_sides_go_idle(mock_clock):
    # Establishes a real tunnel, then neither side sends another byte.
    # Against current proxy.py pipe() has no idle timeout, so both pipe()
    # tasks block forever in receive_some() and the nursery in http_proxy()
    # never exits -- this is the discriminating slowloris test: it fails
    # even if pipe() alone gets a timeout, unless that timeout also tears
    # down the sibling pipe task and closes the tunnel.
    proxy_client, test_client = tt.memory_stream_pair()
    dest_proxy_side, dest_test_side = tt.memory_stream_pair()

    async def fake_open_tcp_stream(_host, _port):
        return dest_proxy_side

    with trio.move_on_after(120) as watchdog:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(
                partial(
                    proxy.http_proxy,
                    proxy_client,
                    _connect=fake_open_tcp_stream,
                    idle_timeout=5,
                )
            )
            await test_client.send_all(connect_request(target=b"example.com:443"))
            reply = await test_client.receive_some(4096)
            assert reply == b"HTTP/1.1 200 Connection established\r\n\r\n"
            # Neither side sends anything from here on -- just wait for
            # http_proxy() (started above) to notice the idle tunnel and
            # return, which ends this nursery block.
    assert not watchdog.cancelled_caught, (
        "http_proxy() never gave up on the idle tunnel"
    )

    # The handler should have closed both streams on its way out.
    remainder = await dest_test_side.receive_some(4096)
    assert remainder == b""


@tt.trio_test
async def test_http_proxy_closes_tunnel_when_dest_stalls_on_send(mock_clock):
    # Client keeps sending; the destination never drains (its send_all_hook
    # never returns, simulating a filled TCP send buffer). Discriminating
    # test for the send-side timeout, same shape as the idle-timeout one
    # above: it fails unless the stalled pipe() also tears down its sibling
    # and closes the whole tunnel, not just the stalled direction.
    proxy_client, test_client = tt.memory_stream_pair()
    dest_proxy_side, _dest_test_side = tt.memory_stream_pair()
    dest_proxy_side.send_stream.send_all_hook = trio.sleep_forever

    async def fake_open_tcp_stream(_host, _port):
        return dest_proxy_side

    with trio.move_on_after(120) as watchdog:
        async with trio.open_nursery() as nursery:
            nursery.start_soon(
                partial(
                    proxy.http_proxy,
                    proxy_client,
                    _connect=fake_open_tcp_stream,
                    # idle_timeout deliberately much larger than send_timeout:
                    # the dest->client direction never receives anything
                    # either, so without this gap its idle timeout could race
                    # the send timeout below and fire first, leaving this
                    # test unable to tell which code path actually closed the
                    # tunnel.
                    idle_timeout=1000,
                    send_timeout=5,
                )
            )
            await test_client.send_all(connect_request(target=b"example.com:443"))
            reply = await test_client.receive_some(4096)
            assert reply == b"HTTP/1.1 200 Connection established\r\n\r\n"
            await test_client.send_all(b"client->dest payload")
            # dest never drains -- wait for http_proxy() to notice the
            # stalled send and return, which ends this nursery block.
    assert not watchdog.cancelled_caught, (
        "http_proxy() never gave up on the stalled destination"
    )


@tt.trio_test
async def test_http_proxy_bad_request_sends_400_and_closes_quietly():
    proxy_client, test_client = tt.memory_stream_pair()
    called = False

    async def fake_open_tcp_stream(_host, _port):
        nonlocal called
        called = True

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            partial(proxy.http_proxy, proxy_client, _connect=fake_open_tcp_stream)
        )
        await test_client.send_all(b"not a valid request\r\n\r\n")
        reply = await test_client.receive_some(4096)
        assert reply == b"HTTP/1.1 400 Bad Request\r\n\r\n"
        remainder = await test_client.receive_some(4096)
        assert remainder == b""

    assert called is False


@tt.trio_test
async def test_http_proxy_dest_connect_failure_sends_502_and_closes_quietly():
    proxy_client, test_client = tt.memory_stream_pair()

    async def fake_open_tcp_stream(_host, _port):
        raise OSError("no route to host")

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            partial(proxy.http_proxy, proxy_client, _connect=fake_open_tcp_stream)
        )
        await test_client.send_all(connect_request(target=b"example.com:443"))
        reply = await test_client.receive_some(4096)
        assert reply == b"HTTP/1.1 502 Bad Gateway\r\n\r\n"
        remainder = await test_client.receive_some(4096)
        assert remainder == b""


@tt.trio_test
async def test_http_proxy_logs_peer_disconnected_when_dest_close_breaks(caplog):
    # dest_stream.aclose() raises directly from
    # `async with dest_stream, trio.open_nursery() as nursery:`'s __aexit__,
    # outside the nursery, so it's a bare (non-group) exception. except* still
    # matches a bare exception (treats it as a singleton group) same as the
    # nursery-wrapped case exercised below. We drive this with a real
    # MemorySendStream close_hook rather than wrapping the stream, so both
    # real pipe() tasks run and reach a normal EOF before the close breaks.
    proxy_client, test_client = tt.memory_stream_pair()
    dest_proxy_side, dest_test_side = tt.memory_stream_pair()

    def raise_on_close():
        raise trio.BrokenResourceError("peer went away")

    dest_proxy_side.send_stream.close_hook = raise_on_close

    async def fake_open_tcp_stream(_host, _port):
        return dest_proxy_side

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            partial(proxy.http_proxy, proxy_client, _connect=fake_open_tcp_stream)
        )
        await test_client.send_all(connect_request(target=b"example.com:443"))
        await test_client.receive_some(4096)
        # Let both real pipe() tasks hit EOF and return normally, so the
        # nursery inside http_proxy finishes and control reaches dest_stream's
        # __aexit__ -- which is where close_hook raises.
        await test_client.aclose()
        await dest_test_side.aclose()

    assert "Peer disconnected" in caplog.text
    assert "Traceback" not in caplog.text


@tt.trio_test
async def test_http_proxy_logs_peer_disconnected_when_one_pipe_task_breaks(caplog):
    # Regression test: a BrokenResourceError raised *inside* a pipe() task
    # (the common, realistic case -- e.g. the client resets the connection
    # mid-transfer) is delivered via trio's nursery ExceptionGroup. This
    # requires `except*` (not a bare `except`) to be caught quietly. Driven
    # here by a real pipe() task hitting a receive_some_hook that raises,
    # rather than monkeypatching pipe() itself out of the picture.
    proxy_client, test_client = tt.memory_stream_pair()
    dest_proxy_side, _ = tt.memory_stream_pair()

    async def raise_broken():
        raise trio.BrokenResourceError("peer went away")

    # Set before the pipe task ever calls receive_some(): the hook only
    # fires on the *next* call, so it must be in place before that task's
    # first (blocking) receive_some() is already in flight. dest_proxy_side
    # is never touched during the CONNECT handshake, so this is race-free.
    dest_proxy_side.receive_stream.receive_some_hook = raise_broken

    async def fake_open_tcp_stream(_host, _port):
        return dest_proxy_side

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            partial(proxy.http_proxy, proxy_client, _connect=fake_open_tcp_stream)
        )
        await test_client.send_all(connect_request(target=b"example.com:443"))
        await test_client.receive_some(4096)

    assert "Peer disconnected" in caplog.text
    assert "ExceptionGroup" not in caplog.text


@tt.trio_test
async def test_http_proxy_mixed_exception_group_logs_both_handlers(caplog):
    # When the two pipe() tasks fail with different exception types, except*
    # runs every matching handler against its own subgroup -- so a mixed
    # group produces BOTH the quiet line and a traceback, not just one.
    # Both real pipe() tasks run; each direction's stream is hooked to fail
    # with a different exception. receive_some() checkpoints twice before
    # calling the hook, so without synchronization the first task to raise
    # can get its sibling cancelled before that one ever reaches its own
    # hook. Gate both hooks on an Event and release it only once both pipe
    # tasks are confirmed blocked there, so they raise together.
    proxy_client, test_client = tt.memory_stream_pair()
    dest_proxy_side, _ = tt.memory_stream_pair()
    client_handshake_done = False
    release = trio.Event()

    async def fake_open_tcp_stream(_host, _port):
        return dest_proxy_side

    async def client_receive_hook():
        # The first call is the CONNECT handshake read; only calls after
        # that belong to the client->dest pipe stage.
        nonlocal client_handshake_done
        if not client_handshake_done:
            client_handshake_done = True
            return
        await release.wait()
        raise trio.BrokenResourceError("peer went away")

    async def dest_receive_hook():
        await release.wait()
        raise RuntimeError("boom, unrelated bug")

    proxy_client.receive_stream.receive_some_hook = client_receive_hook
    dest_proxy_side.receive_stream.receive_some_hook = dest_receive_hook

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            partial(proxy.http_proxy, proxy_client, _connect=fake_open_tcp_stream)
        )
        await test_client.send_all(connect_request(target=b"example.com:443"))
        await test_client.receive_some(4096)
        await tt.wait_all_tasks_blocked()
        release.set()

    assert "Peer disconnected" in caplog.text
    assert "RuntimeError" in caplog.text
    assert "boom, unrelated bug" in caplog.text


@tt.trio_test
async def test_http_proxy_swallows_unexpected_exception_and_logs_traceback(caplog):
    proxy_client, test_client = tt.memory_stream_pair()
    dest_proxy_side, _ = tt.memory_stream_pair()

    async def raise_bug():
        raise RuntimeError("boom, unrelated bug")

    # Set before the nursery starts, same race-avoidance reasoning as
    # test_http_proxy_logs_peer_disconnected_when_one_pipe_task_breaks.
    dest_proxy_side.receive_stream.receive_some_hook = raise_bug

    async def fake_open_tcp_stream(_host, _port):
        return dest_proxy_side

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            partial(proxy.http_proxy, proxy_client, _connect=fake_open_tcp_stream)
        )
        await test_client.send_all(connect_request(target=b"example.com:443"))
        await test_client.receive_some(4096)

    assert "RuntimeError" in caplog.text
    assert "boom, unrelated bug" in caplog.text


@tt.trio_test
async def test_http_proxy_second_connection_waits_when_limiter_full():
    limiter = trio.CapacityLimiter(1)
    proxy_client1, test_client1 = tt.memory_stream_pair()
    proxy_client2, test_client2 = tt.memory_stream_pair()

    async def hang_connect(_host, _port):
        await trio.sleep_forever()

    connect2_started = False

    async def fake_connect_2(_host, _port):
        nonlocal connect2_started
        connect2_started = True
        await trio.sleep_forever()

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            partial(
                proxy.http_proxy,
                proxy_client1,
                _connect=hang_connect,
                limiter=limiter,
            )
        )
        await test_client1.send_all(connect_request())
        # conn1 is now blocked inside tunnel()'s _connect call, holding the
        # single limiter slot.
        await tt.wait_all_tasks_blocked()

        nursery.start_soon(
            partial(
                proxy.http_proxy,
                proxy_client2,
                _connect=fake_connect_2,
                limiter=limiter,
            )
        )
        await test_client2.send_all(connect_request())
        await tt.wait_all_tasks_blocked()

        # conn2 never even reaches _connect -- it's parked waiting for the
        # limiter, proving the cap provides backpressure rather than just
        # rejecting outright.
        assert connect2_started is False
        assert limiter.statistics().borrowed_tokens == 1
        assert limiter.statistics().tasks_waiting == 1

        nursery.cancel_scope.cancel()


@tt.trio_test
async def test_http_proxy_releases_limiter_slot_when_connection_completes():
    limiter = trio.CapacityLimiter(1)
    proxy_client1, test_client1 = tt.memory_stream_pair()
    proxy_client2, test_client2 = tt.memory_stream_pair()

    async def fake_connect_1(_host, _port):
        raise OSError("no route to host")

    connect2_started = False

    async def fake_connect_2(_host, _port):
        nonlocal connect2_started
        connect2_started = True
        raise OSError("no route to host")

    async with trio.open_nursery() as nursery:
        nursery.start_soon(
            partial(
                proxy.http_proxy,
                proxy_client1,
                _connect=fake_connect_1,
                limiter=limiter,
            )
        )
        nursery.start_soon(
            partial(
                proxy.http_proxy,
                proxy_client2,
                _connect=fake_connect_2,
                limiter=limiter,
            )
        )

        # Feed both CONNECT requests up front: whichever task wins the
        # limiter race, its bytes are already waiting, so nothing blocks on
        # process_as_connect_request()'s real (non-mock-clock) read timeout
        # while the *other* connection's request is still in flight.
        await test_client1.send_all(connect_request())
        await test_client2.send_all(connect_request())

        reply1 = await test_client1.receive_some(4096)
        assert reply1 == b"HTTP/1.1 502 Bad Gateway\r\n\r\n"

        # conn1 released the limiter on its way out, so conn2 -- which was
        # parked waiting for a slot -- can now proceed.
        reply2 = await test_client2.receive_some(4096)
        assert reply2 == b"HTTP/1.1 502 Bad Gateway\r\n\r\n"

    assert connect2_started is True


@tt.trio_test
async def test_http_proxy_assigns_sequential_connection_ids(caplog):
    counter = iter(range(100, 103))

    async def fake_open_tcp_stream(_host, _port):
        raise OSError("irrelevant, just need tunnel() to short-circuit")

    for _ in range(3):
        proxy_client, test_client = tt.memory_stream_pair()
        await test_client.send_all(connect_request(target=b"example.com:443"))
        await proxy.http_proxy(
            proxy_client, _nextid=counter.__next__, _connect=fake_open_tcp_stream
        )

    assert "[conn100]" in caplog.text
    assert "[conn101]" in caplog.text
    assert "[conn102]" in caplog.text


@tt.trio_test
async def test_http_proxy_handles_concurrent_connections_without_cross_talk():
    # Runs 3 http_proxy() tasks truly concurrently (one nursery, all started
    # before any of them is awaited to completion) rather than one-at-a-time
    # like test_http_proxy_assigns_sequential_connection_ids. Each
    # connection's ContextVars (CV_CONN_ID, CV_CLIENT_STREAM, CV_DEST_STREAM,
    # CV_DESTHOST) are set via trio's per-task context copying; if that
    # isolation were broken, one connection's state would leak into another's
    # log lines, or -- worse -- payloads could be forwarded to the wrong
    # destination.
    conns = []
    for i in range(3):
        proxy_client, test_client = tt.memory_stream_pair()
        dest_proxy_side, dest_test_side = tt.memory_stream_pair()
        conns.append((i, proxy_client, test_client, dest_proxy_side, dest_test_side))

    def fake_open_tcp_stream_for(dest_proxy_side):
        async def _connect(_host, _port):
            return dest_proxy_side

        return _connect

    async with trio.open_nursery() as nursery:
        for i, proxy_client, _test_client, dest_proxy_side, _dest_test_side in conns:
            connect = fake_open_tcp_stream_for(dest_proxy_side)
            nursery.start_soon(
                partial(proxy.http_proxy, proxy_client, _connect=connect)
            )

        for i, _proxy_client, test_client, _dest_proxy_side, _dest_test_side in conns:
            await test_client.send_all(
                connect_request(target=f"dest{i}.example.com:443".encode())
            )
            reply = await test_client.receive_some(4096)
            assert reply == b"HTTP/1.1 200 Connection established\r\n\r\n"

        # Send a distinct, identifiable payload down each tunnel, all
        # in-flight at once, then confirm each arrives only at its own
        # destination -- never at a sibling connection's.
        for i, _proxy_client, test_client, _dest_proxy_side, _dest_test_side in conns:
            await test_client.send_all(f"payload-for-conn-{i}".encode())

        for i, _proxy_client, _test_client, _dest_proxy_side, dest_test_side in conns:
            received = await dest_test_side.receive_some(4096)
            assert received == f"payload-for-conn-{i}".encode()

        for _i, _proxy_client, test_client, _dest_proxy_side, dest_test_side in conns:
            await test_client.aclose()
            await dest_test_side.aclose()


# ---------------------------------------------------------------------------
# log / logger
# ---------------------------------------------------------------------------


def test_log_with_no_client_stream_logs_bare_message(caplog):
    def body():
        proxy.logger.info("hello")

    run_isolated(body)
    assert "hello" in caplog.text
    assert "[conn" not in caplog.text


def test_log_with_client_stream_only_logs_conn_prefix(caplog):
    def body():
        proxy.CV_CONN_ID.set(7)
        proxy.CV_CLIENT_STREAM.set(fake_stream())
        proxy.logger.info("hello")

    run_isolated(body)
    assert "[conn7] hello" in caplog.text


def test_log_with_dest_but_no_pipe_from_shows_bidirectional_arrow(caplog):
    def body():
        proxy.CV_CONN_ID.set(1)
        proxy.CV_CLIENT_STREAM.set(fake_stream())
        proxy.CV_DEST_STREAM.set(fake_stream())
        proxy.CV_DESTHOST.set("example.com")
        proxy.logger.info("hello")

    run_isolated(body)
    assert "[conn1 <> example.com] hello" in caplog.text


def test_log_direction_arrow_points_away_from_client_when_piping_from_client(caplog):
    def body():
        client = fake_stream()
        proxy.CV_CONN_ID.set(2)
        proxy.CV_CLIENT_STREAM.set(client)
        proxy.CV_DEST_STREAM.set(fake_stream())
        proxy.CV_DESTHOST.set("example.com")
        proxy.CV_PIPE_FROM.set(client)
        proxy.logger.info("hello")

    run_isolated(body)
    assert "[conn2 -> example.com] hello" in caplog.text


def test_log_direction_arrow_points_toward_client_when_piping_from_dest(caplog):
    def body():
        client = fake_stream()
        dest = fake_stream()
        proxy.CV_CONN_ID.set(3)
        proxy.CV_CLIENT_STREAM.set(client)
        proxy.CV_DEST_STREAM.set(dest)
        proxy.CV_DESTHOST.set("example.com")
        proxy.CV_PIPE_FROM.set(dest)
        proxy.logger.info("hello")

    run_isolated(body)
    assert "[conn3 <- example.com] hello" in caplog.text


def test_log_injects_structured_context_into_record_extra(caplog):
    def body():
        client = fake_stream()
        dest = fake_stream()
        proxy.CV_CONN_ID.set(42)
        proxy.CV_CLIENT_STREAM.set(client)
        proxy.CV_DEST_STREAM.set(dest)
        proxy.CV_DESTHOST.set("structured.example.com")
        proxy.CV_PIPE_FROM.set(client)
        proxy.logger.info("structured check")

    run_isolated(body)
    record = next(r for r in caplog.records if "structured check" in r.message)
    assert record.conn_id == 42
    assert record.desthost == "structured.example.com"
    assert record.direction == "->"
    assert record.conn_prefix == "[conn42 -> structured.example.com]"


def test_log_backwards_compatible_helper(caplog):
    def body():
        proxy.CV_CONN_ID.set(99)
        proxy.CV_CLIENT_STREAM.set(fake_stream())
        proxy.log("compat", "message", level=logging.WARNING, end="")

    run_isolated(body)
    assert "[conn99] compat message" in caplog.text
    record = next(r for r in caplog.records if "compat message" in r.message)
    assert record.levelno == logging.WARNING


def test_format_conn_prefix_helper():
    def body():
        assert proxy.format_conn_prefix() == ""
        client = fake_stream()
        proxy.CV_CLIENT_STREAM.set(client)
        proxy.CV_CONN_ID.set(10)
        assert proxy.format_conn_prefix() == "[conn10]"
        proxy.CV_DEST_STREAM.set(fake_stream())
        proxy.CV_DESTHOST.set("example.org")
        assert proxy.format_conn_prefix() == "[conn10 <> example.org]"
        proxy.CV_PIPE_FROM.set(client)
        assert proxy.format_conn_prefix() == "[conn10 -> example.org]"

    run_isolated(body)


# ---------------------------------------------------------------------------
# start_server
# ---------------------------------------------------------------------------


@tt.trio_test
async def test_start_server_calls_serve_tcp_with_server_and_port(caplog):
    calls = []

    async def fake_serve_tcp(server, port):
        calls.append((server, port))

    async def marker():
        pass

    await proxy.start_server(server=marker, port=9999, _serve=fake_serve_tcp)

    assert calls == [(marker, 9999)]
    assert "Starting marker on port 9999" in caplog.text


@tt.trio_test
async def test_start_server_reports_unspecified_port_as_os_selected(caplog):
    async def fake_serve_tcp(server, port):
        pass

    async def marker():
        pass

    await proxy.start_server(server=marker, port=0, _serve=fake_serve_tcp)

    assert "(OS-selected port)" in caplog.text


@tt.trio_test
async def test_start_server_swallows_keyboard_interrupt(caplog):
    async def fake_serve_tcp(server, port):
        raise KeyboardInterrupt

    await proxy.start_server(server=proxy.http_proxy, port=1234, _serve=fake_serve_tcp)

    assert "Goodbye for now." in caplog.text


# ---------------------------------------------------------------------------
# module-level PORT configuration
# ---------------------------------------------------------------------------


def test_port_defaults_to_8080_when_env_unset():
    assert (
        proxy._int_env(
            "PORT", proxy.DEFAULT_PORT, _getenv=lambda _name, default: default
        )
        == 8080
    )


def test_port_reads_from_env_var():
    assert (
        proxy._int_env(
            "PORT", proxy.DEFAULT_PORT, _getenv=lambda _name, _default: "9191"
        )
        == 9191
    )


# ---------------------------------------------------------------------------
# _int_env (backs BUFMAXSZ / MAXREQSZ)
# ---------------------------------------------------------------------------


def test_int_env_uses_default_when_var_unset():
    assert proxy._int_env("X", 42, _getenv=lambda _name, default: default) == 42


def test_int_env_reads_value_from_env():
    assert proxy._int_env("X", 42, _getenv=lambda _name, _default: "4096") == 4096


# ---------------------------------------------------------------------------
# MAXCONNS / connection cap
# ---------------------------------------------------------------------------


def test_maxconns_defaults_to_1000_when_env_unset():
    assert (
        proxy._int_env(
            "MAXCONNS", proxy.DEFAULT_MAXCONNS, _getenv=lambda _name, default: default
        )
        == 1000
    )


# ---------------------------------------------------------------------------
# _float_env (backs CONNECT_READ_TIMEOUT / DEST_CONNECT_TIMEOUT / PIPE_IDLE_TIMEOUT)
# ---------------------------------------------------------------------------


def test_float_env_uses_default_when_var_unset():
    assert proxy._float_env("X", 4.2, _getenv=lambda _name, default: default) == 4.2


def test_float_env_reads_value_from_env():
    assert proxy._float_env("X", 4.2, _getenv=lambda _name, _default: "1.5") == 1.5


# ---------------------------------------------------------------------------
# Logging levels, exception info, and adapter operations
# ---------------------------------------------------------------------------


@tt.trio_test
async def test_tunnel_logs_warning_with_exc_info_on_bad_request(caplog):
    proxy_client, test_client = tt.memory_stream_pair()
    await test_client.send_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
    with pytest.raises(proxy.ProxyProtocolError):
        await proxy.tunnel(proxy_client)

    rec = next(r for r in caplog.records if "Bad CONNECT request" in r.message)
    assert rec.levelname == "WARNING"
    assert rec.exc_info is not None
    assert rec.exc_info[0] is proxy.ProxyProtocolError


@tt.trio_test
async def test_tunnel_logs_warning_with_exc_info_on_oversized_request(caplog):
    proxy_client, test_client = tt.memory_stream_pair()
    oversized = b"a" * (proxy.DEFAULT_MAXREQSZ + 1) + b":443"
    await test_client.send_all(connect_request(target=oversized))
    with pytest.raises(proxy.RequestTooLargeError):
        await proxy.tunnel(proxy_client)

    rec = next(r for r in caplog.records if "Oversized CONNECT request" in r.message)
    assert rec.levelname == "WARNING"
    assert rec.exc_info is not None
    assert rec.exc_info[0] is proxy.RequestTooLargeError


@tt.trio_test
async def test_tunnel_logs_exception_on_dest_connect_failure(caplog):
    proxy_client, test_client = tt.memory_stream_pair()

    async def fake_connect(_host, _port):
        raise OSError("network is unreachable")

    await test_client.send_all(connect_request(target=b"example.com:443"))
    with pytest.raises(OSError):
        await proxy.tunnel(proxy_client, _connect=fake_connect)

    rec = next(r for r in caplog.records if "Could not connect to" in r.message)
    assert rec.levelname == "ERROR"
    assert rec.exc_info is not None
    assert rec.exc_info[0] is OSError


@tt.trio_test
async def test_tunnel_logs_exception_on_dest_connect_timeout(caplog, mock_clock):
    proxy_client, test_client = tt.memory_stream_pair()

    async def hang(_host, _port):
        await trio.sleep_forever()

    await test_client.send_all(connect_request(target=b"example.com:443"))
    with pytest.raises(trio.TooSlowError):
        await proxy.tunnel(proxy_client, _connect=hang, dest_connect_timeout=1)

    rec = next(r for r in caplog.records if "Timed out connecting to" in r.message)
    assert rec.levelname == "ERROR"
    assert rec.exc_info is not None


@tt.trio_test
async def test_pipe_with_timeout_logs_warning_on_timeout(caplog, mock_clock):
    from_proxy, _from_test = tt.memory_stream_pair()
    to_proxy, _to_test = tt.memory_stream_pair()

    with pytest.raises(trio.TooSlowError):
        await proxy.pipe_with_timeout(from_proxy, to_proxy, idle_timeout=2)

    rec = next(r for r in caplog.records if "closing tunnel" in r.message)
    assert rec.levelname == "WARNING"
    assert "Pipe idle for 2s" in rec.message


def test_logger_adapter_delegates_handlers_and_filters():
    test_handler = logging.NullHandler()
    test_filter = logging.Filter()

    proxy.logger.addHandler(test_handler)
    assert test_handler in proxy.logger.logger.handlers
    proxy.logger.removeHandler(test_handler)
    assert test_handler not in proxy.logger.logger.handlers

    proxy.logger.addFilter(test_filter)
    assert test_filter in proxy.logger.logger.filters
    proxy.logger.removeFilter(test_filter)
    assert test_filter not in proxy.logger.logger.filters


def test_log_level_defaults_and_env():
    assert proxy.DEFAULT_LOG_LEVEL == "INFO"
    assert proxy.LOG_LEVEL in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


def test_log_with_no_args_is_noop(caplog):
    proxy.log()
    assert len(caplog.records) == 0


def test_raw_logger_filter_populates_attributes(caplog):
    def body():
        client = fake_stream()
        proxy.CV_CONN_ID.set(55)
        proxy.CV_CLIENT_STREAM.set(client)
        proxy._raw_logger.info("raw logger message")

    run_isolated(body)
    rec = next(r for r in caplog.records if "raw logger message" in r.message)
    assert rec.conn_id == 55
    assert rec.conn_prefix == "[conn55]"
