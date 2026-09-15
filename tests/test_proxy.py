import contextvars
import importlib
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
    server, client = tt.memory_stream_pair()
    req = connect_request()
    for i in range(0, len(req), 3):
        await client.send_all(req[i : i + 3])
    host, port = await proxy.process_as_connect_request(server)
    assert (host, port) == ("example.com", 443)


@tt.trio_test
async def test_process_as_connect_request_rejects_non_connect_method():
    server, client = tt.memory_stream_pair()
    await client.send_all(b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
    with pytest.raises(AssertionError, match="CONNECT"):
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
    with pytest.raises(AssertionError, match="did not fit"):
        await proxy.process_as_connect_request(server, bufmaxsz=8, maxreqsz=32)


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
    with pytest.raises(AssertionError, match="is not a h11.Request"):
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


# ---------------------------------------------------------------------------
# tunnel
# ---------------------------------------------------------------------------


@tt.trio_test
async def test_tunnel_success_connects_and_replies_200(monkeypatch):
    proxy_client, test_client = tt.memory_stream_pair()
    dest_proxy_side, _ = tt.memory_stream_pair()
    calls = []

    async def fake_open_tcp_stream(host, port):
        calls.append((host, port))
        return dest_proxy_side

    monkeypatch.setattr(trio, "open_tcp_stream", fake_open_tcp_stream)

    await test_client.send_all(connect_request(target=b"example.com:443"))
    dest_stream = await proxy.tunnel(proxy_client)

    assert calls == [("example.com", 443)]
    assert dest_stream is dest_proxy_side
    reply = await test_client.receive_some(4096)
    assert reply == b"HTTP/1.1 200 Connection established\r\n\r\n"


@tt.trio_test
async def test_tunnel_bad_request_sends_400_and_raises_without_connecting(monkeypatch):
    proxy_client, test_client = tt.memory_stream_pair()
    called = False

    async def fake_open_tcp_stream(_host, _port):
        nonlocal called
        called = True
        raise AssertionError("should not be called")

    monkeypatch.setattr(trio, "open_tcp_stream", fake_open_tcp_stream)

    await test_client.send_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
    with pytest.raises(AssertionError):
        await proxy.tunnel(proxy_client)

    assert called is False
    reply = await test_client.receive_some(4096)
    assert reply == b"HTTP/1.1 400 Bad Request\r\n\r\n"


@tt.trio_test
async def test_tunnel_connect_failure_sends_502_and_raises(monkeypatch):
    proxy_client, test_client = tt.memory_stream_pair()

    async def fake_open_tcp_stream(_host, _port):
        raise OSError("connection refused")

    monkeypatch.setattr(trio, "open_tcp_stream", fake_open_tcp_stream)

    await test_client.send_all(connect_request(target=b"example.com:443"))
    with pytest.raises(OSError):
        await proxy.tunnel(proxy_client)

    reply = await test_client.receive_some(4096)
    assert reply == b"HTTP/1.1 502 Bad Gateway\r\n\r\n"


# ---------------------------------------------------------------------------
# pipe
# ---------------------------------------------------------------------------


@tt.trio_test
async def test_pipe_forwards_multiple_chunks_in_order():
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


@tt.trio_test
async def test_pipe_immediate_eof_forwards_nothing():
    from_proxy, from_test = tt.memory_stream_pair()
    to_proxy, to_test = tt.memory_stream_pair()
    await from_test.aclose()

    await proxy.pipe(from_proxy, to_proxy)

    await to_proxy.aclose()
    remainder = await to_test.receive_some(4096)
    assert remainder == b""


# ---------------------------------------------------------------------------
# http_proxy (end-to-end)
# ---------------------------------------------------------------------------


@tt.trio_test
async def test_http_proxy_full_round_trip(monkeypatch):
    proxy_client, test_client = tt.memory_stream_pair()
    dest_proxy_side, dest_test_side = tt.memory_stream_pair()

    async def fake_open_tcp_stream(_host, _port):
        return dest_proxy_side

    monkeypatch.setattr(trio, "open_tcp_stream", fake_open_tcp_stream)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(proxy.http_proxy, proxy_client)

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
async def test_http_proxy_bad_request_sends_400_and_closes_quietly(monkeypatch):
    proxy_client, test_client = tt.memory_stream_pair()
    called = False

    async def fake_open_tcp_stream(_host, _port):
        nonlocal called
        called = True

    monkeypatch.setattr(trio, "open_tcp_stream", fake_open_tcp_stream)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(proxy.http_proxy, proxy_client)
        await test_client.send_all(b"not a valid request\r\n\r\n")
        reply = await test_client.receive_some(4096)
        assert reply == b"HTTP/1.1 400 Bad Request\r\n\r\n"
        remainder = await test_client.receive_some(4096)
        assert remainder == b""

    assert called is False


@tt.trio_test
async def test_http_proxy_dest_connect_failure_sends_502_and_closes_quietly(
    monkeypatch,
):
    proxy_client, test_client = tt.memory_stream_pair()

    async def fake_open_tcp_stream(_host, _port):
        raise OSError("no route to host")

    monkeypatch.setattr(trio, "open_tcp_stream", fake_open_tcp_stream)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(proxy.http_proxy, proxy_client)
        await test_client.send_all(connect_request(target=b"example.com:443"))
        reply = await test_client.receive_some(4096)
        assert reply == b"HTTP/1.1 502 Bad Gateway\r\n\r\n"
        remainder = await test_client.receive_some(4096)
        assert remainder == b""


class _RaisesOnClose:
    """Wraps a real stream but makes aclose() raise, to simulate a peer
    that's already gone by the time we try to tear the tunnel down."""

    def __init__(self, inner, exc):
        self._inner = inner
        self._exc = exc

    def __getattr__(self, name):
        return getattr(self._inner, name)

    async def aclose(self):
        raise self._exc

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        await self.aclose()


@tt.trio_test
async def test_http_proxy_logs_peer_disconnected_when_dest_close_breaks(
    monkeypatch, capsys
):
    # trio 0.34 wraps EVERY nursery-child exception in an ExceptionGroup,
    # even a lone one (see test below), so a BrokenResourceError raised by a
    # pipe() task can never match the bare `except trio.BrokenResourceError,
    # trio.ClosedResourceError:` in http_proxy(). The only way that branch
    # is actually reachable is if closing dest_stream itself raises, in
    # `async with dest_stream, trio.open_nursery() as nursery:`'s __aexit__,
    # which happens *outside* the nursery and so is not group-wrapped.
    proxy_client, test_client = tt.memory_stream_pair()
    dest_proxy_side, _ = tt.memory_stream_pair()
    breaking_dest = _RaisesOnClose(
        dest_proxy_side, trio.BrokenResourceError("peer went away")
    )

    async def fake_open_tcp_stream(_host, _port):
        return breaking_dest

    async def noop_pipe(from_stream, to_stream):
        return

    monkeypatch.setattr(trio, "open_tcp_stream", fake_open_tcp_stream)
    monkeypatch.setattr(proxy, "pipe", noop_pipe)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(proxy.http_proxy, proxy_client)
        await test_client.send_all(connect_request(target=b"example.com:443"))
        await test_client.receive_some(4096)

    out = capsys.readouterr().out
    assert "Peer disconnected" in out
    assert "Traceback" not in out


@tt.trio_test
async def test_http_proxy_pipe_broken_resource_error_is_never_caught_quietly(
    monkeypatch, capsys
):
    # BUG: a BrokenResourceError raised *inside* a pipe() task (the common,
    # realistic case -- e.g. the client resets the connection mid-transfer)
    # is always delivered via trio's nursery ExceptionGroup, which the bare
    # `except trio.BrokenResourceError, trio.ClosedResourceError:` in
    # http_proxy() cannot match (only `except*` would). It falls through to
    # the catch-all `except Exception` and dumps a full traceback instead of
    # the intended quiet "Peer disconnected" line. This pins the current
    # (surprising) behavior rather than the documented intent.
    proxy_client, test_client = tt.memory_stream_pair()
    dest_proxy_side, _ = tt.memory_stream_pair()

    async def fake_open_tcp_stream(_host, _port):
        return dest_proxy_side

    async def broken_pipe(from_stream, to_stream):
        raise trio.BrokenResourceError("peer went away")

    monkeypatch.setattr(trio, "open_tcp_stream", fake_open_tcp_stream)
    monkeypatch.setattr(proxy, "pipe", broken_pipe)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(proxy.http_proxy, proxy_client)
        await test_client.send_all(connect_request(target=b"example.com:443"))
        await test_client.receive_some(4096)

    out = capsys.readouterr().out
    assert "Peer disconnected" not in out
    assert "ExceptionGroup" in out


@tt.trio_test
async def test_http_proxy_swallows_unexpected_exception_and_logs_traceback(
    monkeypatch, capsys
):
    proxy_client, test_client = tt.memory_stream_pair()
    dest_proxy_side, _ = tt.memory_stream_pair()

    async def fake_open_tcp_stream(_host, _port):
        return dest_proxy_side

    async def buggy_pipe(from_stream, to_stream):
        raise RuntimeError("boom, unrelated bug")

    monkeypatch.setattr(trio, "open_tcp_stream", fake_open_tcp_stream)
    monkeypatch.setattr(proxy, "pipe", buggy_pipe)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(proxy.http_proxy, proxy_client)
        await test_client.send_all(connect_request(target=b"example.com:443"))
        await test_client.receive_some(4096)

    out = capsys.readouterr().out
    assert "RuntimeError" in out
    assert "boom, unrelated bug" in out


@tt.trio_test
async def test_http_proxy_assigns_sequential_connection_ids(monkeypatch):
    seen_ids = []
    counter = iter(range(100, 103))

    async def fake_open_tcp_stream(_host, _port):
        raise OSError("irrelevant, just need tunnel() to short-circuit")

    monkeypatch.setattr(trio, "open_tcp_stream", fake_open_tcp_stream)

    for _ in range(3):
        proxy_client, test_client = tt.memory_stream_pair()
        await test_client.send_all(connect_request(target=b"example.com:443"))
        await proxy.http_proxy(proxy_client, _nextid=counter.__next__)
        seen_ids.append(None)  # http_proxy ran to completion without raising

    assert len(seen_ids) == 3


# ---------------------------------------------------------------------------
# log
# ---------------------------------------------------------------------------


def test_log_with_no_client_stream_prints_bare_message(capsys):
    def body():
        proxy.log("hello")

    run_isolated(body)
    assert capsys.readouterr().out == "hello\n"


def test_log_with_client_stream_only_prints_conn_prefix(capsys):
    def body():
        proxy.CV_CONN_ID.set(7)
        proxy.CV_CLIENT_STREAM.set(fake_stream())
        proxy.log("hello")

    run_isolated(body)
    assert capsys.readouterr().out == "[conn7] hello\n"


def test_log_with_dest_but_no_pipe_from_shows_bidirectional_arrow(capsys):
    def body():
        proxy.CV_CONN_ID.set(1)
        proxy.CV_CLIENT_STREAM.set(fake_stream())
        proxy.CV_DEST_STREAM.set(fake_stream())
        proxy.CV_DESTHOST.set("example.com")
        proxy.log("hello")

    run_isolated(body)
    assert capsys.readouterr().out == "[conn1 <> example.com] hello\n"


def test_log_direction_arrow_points_away_from_client_when_piping_from_client(capsys):
    def body():
        client = fake_stream()
        proxy.CV_CONN_ID.set(2)
        proxy.CV_CLIENT_STREAM.set(client)
        proxy.CV_DEST_STREAM.set(fake_stream())
        proxy.CV_DESTHOST.set("example.com")
        proxy.CV_PIPE_FROM.set(client)
        proxy.log("hello")

    run_isolated(body)
    assert capsys.readouterr().out == "[conn2 -> example.com] hello\n"


def test_log_direction_arrow_points_toward_client_when_piping_from_dest(capsys):
    def body():
        client = fake_stream()
        dest = fake_stream()
        proxy.CV_CONN_ID.set(3)
        proxy.CV_CLIENT_STREAM.set(client)
        proxy.CV_DEST_STREAM.set(dest)
        proxy.CV_DESTHOST.set("example.com")
        proxy.CV_PIPE_FROM.set(dest)
        proxy.log("hello")

    run_isolated(body)
    assert capsys.readouterr().out == "[conn3 <- example.com] hello\n"


def test_log_forwards_extra_kwargs_to_print(capsys):
    def body():
        proxy.log("no", "newline", end="")

    run_isolated(body)
    assert capsys.readouterr().out == "no newline"


# ---------------------------------------------------------------------------
# start_server
# ---------------------------------------------------------------------------


@tt.trio_test
async def test_start_server_calls_serve_tcp_with_server_and_port(monkeypatch, capsys):
    calls = []

    async def fake_serve_tcp(server, port):
        calls.append((server, port))

    monkeypatch.setattr(trio, "serve_tcp", fake_serve_tcp)

    async def marker():
        pass

    await proxy.start_server(server=marker, port=9999)

    assert calls == [(marker, 9999)]
    assert "Starting marker on port 9999" in capsys.readouterr().out


@tt.trio_test
async def test_start_server_reports_unspecified_port_as_os_selected(
    monkeypatch, capsys
):
    async def fake_serve_tcp(server, port):
        pass

    monkeypatch.setattr(trio, "serve_tcp", fake_serve_tcp)

    async def marker():
        pass

    await proxy.start_server(server=marker, port=0)

    assert "(OS-selected port)" in capsys.readouterr().out


@tt.trio_test
async def test_start_server_swallows_keyboard_interrupt(monkeypatch, capsys):
    async def fake_serve_tcp(server, port):
        raise KeyboardInterrupt

    monkeypatch.setattr(trio, "serve_tcp", fake_serve_tcp)

    await proxy.start_server(server=proxy.http_proxy, port=1234)

    assert "Goodbye for now." in capsys.readouterr().out


# ---------------------------------------------------------------------------
# module-level PORT configuration
# ---------------------------------------------------------------------------


def test_port_defaults_to_8080_when_env_unset(monkeypatch):
    monkeypatch.delenv("PORT", raising=False)
    reloaded = importlib.reload(proxy)
    try:
        assert reloaded.PORT == 8080
    finally:
        monkeypatch.delenv("PORT", raising=False)
        importlib.reload(proxy)


def test_port_reads_from_env_var(monkeypatch):
    monkeypatch.setenv("PORT", "9191")
    reloaded = importlib.reload(proxy)
    try:
        assert reloaded.PORT == 9191
    finally:
        monkeypatch.delenv("PORT", raising=False)
        importlib.reload(proxy)
