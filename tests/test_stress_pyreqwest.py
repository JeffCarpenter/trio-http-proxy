import asyncio
import hashlib
import os
import ssl
import subprocess
import tempfile
import time
from collections.abc import AsyncIterator, Generator
from contextlib import asynccontextmanager
from datetime import timedelta
from functools import partial
from typing import Any

import pytest
import trio
from pyreqwest.client import Client, ClientBuilder
from pyreqwest.exceptions import PyreqwestError
from pyreqwest.proxy import ProxyBuilder

from trio_http_proxy.errors import CircuitBreakerOpen, RateLimitError
from trio_http_proxy.proxy import http_proxy


@pytest.fixture(scope="module")
def tls_cert_key_pair() -> Generator[tuple[str, str]]:
    """Generate a self-signed certificate and private key using OpenSSL for TLS server tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        cert_path = os.path.join(tmpdir, "cert.pem")
        key_path = os.path.join(tmpdir, "key.pem")
        cmd = [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-keyout",
            key_path,
            "-out",
            cert_path,
            "-days",
            "1",
            "-nodes",
            "-subj",
            "/CN=127.0.0.1",
        ]
        subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        yield cert_path, key_path


@pytest.fixture(scope="module")
def server_ssl_context(tls_cert_key_pair: tuple[str, str]) -> ssl.SSLContext:
    cert_path, key_path = tls_cert_key_pair
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(cert_path, key_path)
    return ctx


async def _handle_origin_request(
    tls_stream: trio.SSLStream[trio.SocketStream],
) -> None:
    try:
        buffer = bytearray()
        while b"\r\n\r\n" not in buffer:
            chunk = await tls_stream.receive_some(8192)
            if not chunk:
                return
            buffer.extend(chunk)

        header_bytes, _, rest = bytes(buffer).partition(b"\r\n\r\n")
        lines = header_bytes.split(b"\r\n")
        req_line = lines[0].decode("latin1", errors="replace")
        parts = req_line.split()
        if len(parts) < 2:
            return
        _, path = parts[0], parts[1]

        content_length = 0
        for line in lines[1:]:
            if line.lower().startswith(b"content-length:"):
                try:
                    content_length = int(line.split(b":", 1)[1].strip())
                except ValueError:
                    content_length = 0

        body = bytearray(rest)
        while len(body) < content_length:
            needed = content_length - len(body)
            chunk = await tls_stream.receive_some(min(65536, needed))
            if not chunk:
                break
            body.extend(chunk)

        if path == "/echo":
            resp_body = b"ECHO:" + bytes(body)
            resp = (
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: application/octet-stream\r\n"
                b"Content-Length: " + str(len(resp_body)).encode() + b"\r\n"
                b"Connection: close\r\n\r\n" + resp_body
            )
            await tls_stream.send_all(resp)
        elif path == "/sha256":
            digest = hashlib.sha256(bytes(body)).hexdigest().encode("ascii")
            resp = (
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: text/plain\r\n"
                b"Content-Length: " + str(len(digest)).encode() + b"\r\n"
                b"Connection: close\r\n\r\n" + digest
            )
            await tls_stream.send_all(resp)
        elif path.startswith("/stream_large/"):
            # /stream_large/<chunk_count>/<chunk_size>
            path_only, _, _ = path.partition("?")
            route_parts = path_only.strip("/").split("/")
            chunk_count = int(route_parts[1])
            chunk_size = int(route_parts[2])
            total_bytes = chunk_count * chunk_size
            header = (
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: application/octet-stream\r\n"
                b"Content-Length: " + str(total_bytes).encode() + b"\r\n"
                b"Connection: close\r\n\r\n"
            )
            await tls_stream.send_all(header)
            data_chunk = b"Z" * chunk_size
            for _ in range(chunk_count):
                await tls_stream.send_all(data_chunk)
        elif path == "/slow_disconnect":
            # Send part of the response then wait before closing to induce client aborts
            await tls_stream.send_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 100000\r\n\r\nSTART_OF_STREAM..."
            )
            await trio.sleep(1.0)
        else:
            resp_body = b"OK"
            resp = (
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: text/plain\r\n"
                b"Content-Length: 2\r\n"
                b"Connection: close\r\n\r\n" + resp_body
            )
            await tls_stream.send_all(resp)
    except trio.BrokenResourceError, trio.ClosedResourceError, ssl.SSLError:
        pass
    finally:
        with trio.move_on_after(0.2):
            await tls_stream.aclose()


async def _tls_origin_server(
    ssl_context: ssl.SSLContext,
    stream: trio.SocketStream,
) -> None:
    try:
        tls_stream = trio.SSLStream(stream, ssl_context, server_side=True)
        await tls_stream.do_handshake()
    except trio.BrokenResourceError, trio.ClosedResourceError, ssl.SSLError:
        return

    await _handle_origin_request(tls_stream)


@asynccontextmanager
async def spawn_proxy_and_origin(
    ssl_context: ssl.SSLContext,
    *,
    limiter: trio.CapacityLimiter | None = None,
    proxy_cache: Any = None,
) -> AsyncIterator[tuple[int, int]]:
    """Spawns an HTTPS origin server and a Trio HTTP proxy server concurrently."""
    async with trio.open_nursery() as nursery:
        origin_handler = partial(_tls_origin_server, ssl_context)
        origin_listeners = await nursery.start(
            partial(trio.serve_tcp, origin_handler, 0)
        )
        origin_port = origin_listeners[0].socket.getsockname()[1]

        proxy_handler = partial(
            http_proxy,
            limiter=limiter,
            proxy_cache=proxy_cache,
        )
        proxy_listeners = await nursery.start(partial(trio.serve_tcp, proxy_handler, 0))
        proxy_port = proxy_listeners[0].socket.getsockname()[1]

        try:
            yield proxy_port, origin_port
        finally:
            nursery.cancel_scope.cancel()


def build_pyreqwest_client(
    proxy_port: int,
    timeout_sec: float = 15.0,
    max_conns: int = 500,
) -> Client:
    proxy = ProxyBuilder.all(f"http://127.0.0.1:{proxy_port}")
    return (
        ClientBuilder()
        .proxy(proxy)
        .danger_accept_invalid_certs(True)
        .connect_timeout(timedelta(seconds=5))
        .timeout(timedelta(seconds=timeout_sec))
        .max_connections(max_conns)
        .build()
    )


@pytest.mark.trio
async def test_pyreqwest_stress_high_concurrency_stampede(
    server_ssl_context: ssl.SSLContext,
) -> None:
    """Hammer the Trio proxy with 500 concurrent HTTPS requests via pyreqwest (asyncio loop)."""
    concurrency = 500

    async with spawn_proxy_and_origin(server_ssl_context) as (proxy_port, dest_port):

        def run_asyncio_load() -> tuple[list[int], float]:
            async def amain() -> tuple[list[int], float]:
                client = build_pyreqwest_client(proxy_port, max_conns=concurrency)
                sem = asyncio.Semaphore(150)

                async def fetch(idx: int) -> int:
                    async with sem:
                        req = client.get(
                            f"https://127.0.0.1:{dest_port}/ping/{idx}"
                        ).build()
                        resp = await req.send()
                        text = await resp.text()
                        assert text == "OK"
                        return resp.status

                t0 = time.monotonic()
                tasks = [fetch(i) for i in range(concurrency)]
                statuses = await asyncio.gather(*tasks)
                elapsed = time.monotonic() - t0
                return statuses, elapsed

            return asyncio.run(amain())

        statuses, elapsed = await trio.to_thread.run_sync(run_asyncio_load)
        assert len(statuses) == concurrency
        assert all(s == 200 for s in statuses)
        # Ensure high throughput performance
        req_per_sec = concurrency / max(elapsed, 0.001)
        assert req_per_sec > 50.0, f"Expected >50 req/s, achieved {req_per_sec:.1f}"


@pytest.mark.trio
async def test_pyreqwest_stress_sustained_heavy_throughput(
    server_ssl_context: ssl.SSLContext,
) -> None:
    """Execute sustained waves totaling 1,000 requests to verify endurance and absence of connection leaks."""
    waves = 5
    wave_size = 200
    total_expected = waves * wave_size

    async with spawn_proxy_and_origin(server_ssl_context) as (proxy_port, dest_port):

        def run_asyncio_waves() -> int:
            async def amain() -> int:
                client = build_pyreqwest_client(proxy_port, max_conns=wave_size)
                total_success = 0

                for wave in range(waves):
                    current_wave = wave

                    async def fetch(w: int, idx: int) -> int:
                        url = f"https://127.0.0.1:{dest_port}/wave_{w}_{idx}"
                        req = client.get(url).build()
                        resp = await req.send()
                        assert resp.status == 200
                        return resp.status

                    results = await asyncio.gather(
                        *[fetch(current_wave, i) for i in range(wave_size)]
                    )
                    total_success += sum(1 for s in results if s == 200)

                return total_success

            return asyncio.run(amain())

        total = await trio.to_thread.run_sync(run_asyncio_waves)
        assert total == total_expected


@pytest.mark.trio
async def test_pyreqwest_stress_large_payload_saturation(
    server_ssl_context: ssl.SSLContext,
) -> None:
    """Stress test the proxy tunnel with 8 concurrent 2MB body transfers (16MB total) verified by SHA-256."""
    payload_size = 2 * 1024 * 1024  # 2 MB per request
    concurrency = 8

    payloads = [os.urandom(payload_size) for _ in range(concurrency)]
    expected_digests = [hashlib.sha256(p).hexdigest() for p in payloads]

    async with spawn_proxy_and_origin(server_ssl_context) as (proxy_port, dest_port):

        def run_asyncio_uploads() -> list[str]:
            async def amain() -> list[str]:
                client = build_pyreqwest_client(proxy_port, timeout_sec=40.0)

                async def upload(idx: int) -> str:
                    data = payloads[idx]
                    req = (
                        client.post(f"https://127.0.0.1:{dest_port}/sha256")
                        .body_bytes(data)
                        .build()
                    )
                    resp = await req.send()
                    assert resp.status == 200
                    return await resp.text()

                tasks = [upload(i) for i in range(concurrency)]
                return await asyncio.gather(*tasks)

            return asyncio.run(amain())

        results = await trio.to_thread.run_sync(run_asyncio_uploads)
        assert results == expected_digests


@pytest.mark.trio
async def test_pyreqwest_stress_streaming_deep_buffer_drain(
    server_ssl_context: ssl.SSLContext,
) -> None:
    """Stream 5 concurrent 6.4MB flows (100 chunks x 64KB = 32MB total) to stress chunked piping and buffer drain."""
    chunk_count = 100
    chunk_size = 64 * 1024  # 64KB chunks -> 6.4MB per stream
    expected_stream_len = chunk_count * chunk_size
    concurrency = 5

    async with spawn_proxy_and_origin(server_ssl_context) as (proxy_port, dest_port):

        def run_asyncio_stream() -> list[int]:
            async def amain() -> list[int]:
                client = build_pyreqwest_client(proxy_port, timeout_sec=30.0)

                async def stream_one(idx: int) -> int:
                    url = f"https://127.0.0.1:{dest_port}/stream_large/{chunk_count}/{chunk_size}?id={idx}"
                    req = client.get(url).build()
                    resp = await req.send()
                    assert resp.status == 200
                    data = await resp.bytes()
                    return len(data)

                tasks = [stream_one(i) for i in range(concurrency)]
                return await asyncio.gather(*tasks)

            return asyncio.run(amain())

        received_lens = await trio.to_thread.run_sync(run_asyncio_stream)
        assert len(received_lens) == concurrency
        assert all(L == expected_stream_len for L in received_lens)


@pytest.mark.trio
async def test_pyreqwest_stress_heavy_limiter_queue_contention(
    server_ssl_context: ssl.SSLContext,
) -> None:
    """Heavily constrain connection capacity (limiter=10) and flood with 150 concurrent requests."""
    concurrency = 150
    limiter = trio.CapacityLimiter(
        10
    )  # Only 10 concurrent proxy tunnels admitted at once

    async with spawn_proxy_and_origin(server_ssl_context, limiter=limiter) as (
        proxy_port,
        dest_port,
    ):

        def run_asyncio_gated() -> list[int]:
            async def amain() -> list[int]:
                client = build_pyreqwest_client(
                    proxy_port, timeout_sec=30.0, max_conns=concurrency
                )

                async def fetch(idx: int) -> int:
                    req = client.get(
                        f"https://127.0.0.1:{dest_port}/gated/{idx}"
                    ).build()
                    resp = await req.send()
                    text = await resp.text()
                    assert text == "OK"
                    return resp.status

                tasks = [fetch(i) for i in range(concurrency)]
                return await asyncio.gather(*tasks)

            return asyncio.run(amain())

        statuses = await trio.to_thread.run_sync(run_asyncio_gated)
        assert len(statuses) == concurrency
        assert all(s == 200 for s in statuses)


@pytest.mark.trio
async def test_pyreqwest_stress_massive_rate_limit_stampede(
    server_ssl_context: ssl.SSLContext,
) -> None:
    """Flood proxy with 250 concurrent requests against a strict 50-request rate limit."""
    rate_limit_allow_count = 50
    total_requests = 250

    class AtomicCountingRateLimitCache:
        def __init__(self, allow_max: int) -> None:
            self.allow_max = allow_max
            self.count = 0

        async def check_rate_limit(self, _key: str) -> None:
            self.count += 1
            if self.count > self.allow_max:
                raise RateLimitError(retry_after=60)

        async def check_circuit(self, _host: str, _port: int) -> None:
            pass

        async def resolve_hosts(self, host: str) -> list[str]:
            return [host]

        async def record_connection_result(self, _record: object) -> None:
            pass

    cache = AtomicCountingRateLimitCache(allow_max=rate_limit_allow_count)

    async with spawn_proxy_and_origin(server_ssl_context, proxy_cache=cache) as (
        proxy_port,
        dest_port,
    ):

        def run_asyncio_rate_limit_load() -> list[int]:
            async def amain() -> list[int]:
                client = build_pyreqwest_client(proxy_port, max_conns=total_requests)

                async def fetch(idx: int) -> int:
                    req = client.get(
                        f"https://127.0.0.1:{dest_port}/rate/{idx}"
                    ).build()
                    try:
                        resp = await req.send()
                        return resp.status
                    except PyreqwestError:
                        # 429 Too Many Requests response early in CONNECT causes tunnel rejection
                        return 429

                tasks = [fetch(i) for i in range(total_requests)]
                return await asyncio.gather(*tasks)

            return asyncio.run(amain())

        results = await trio.to_thread.run_sync(run_asyncio_rate_limit_load)
        success_count = sum(1 for s in results if s == 200)
        rate_limited_count = sum(1 for s in results if s == 429)

        assert success_count == rate_limit_allow_count
        assert success_count + rate_limited_count == total_requests


@pytest.mark.trio
async def test_pyreqwest_stress_circuit_breaker_storm(
    server_ssl_context: ssl.SSLContext,
) -> None:
    """Storm the proxy with 200 concurrent requests against a tripped circuit breaker."""
    total_requests = 200

    class TrippedCircuitCache:
        async def check_rate_limit(self, _key: str) -> None:
            pass

        async def check_circuit(self, _host: str, _port: int) -> None:
            raise CircuitBreakerOpen("Destination circuit tripped")

        async def resolve_hosts(self, host: str) -> list[str]:
            return [host]

        async def record_connection_result(self, _record: object) -> None:
            pass

    async with spawn_proxy_and_origin(
        server_ssl_context, proxy_cache=TrippedCircuitCache()
    ) as (proxy_port, dest_port):

        def run_asyncio_circuit_stress() -> list[int]:
            async def amain() -> list[int]:
                client = build_pyreqwest_client(proxy_port, max_conns=total_requests)

                async def fetch(idx: int) -> int:
                    req = client.get(
                        f"https://127.0.0.1:{dest_port}/circuit/{idx}"
                    ).build()
                    try:
                        resp = await req.send()
                        return resp.status
                    except PyreqwestError:
                        # HTTP 502 Bad Gateway closes the CONNECT tunnel
                        return 502

                tasks = [fetch(i) for i in range(total_requests)]
                return await asyncio.gather(*tasks)

            return asyncio.run(amain())

        statuses = await trio.to_thread.run_sync(run_asyncio_circuit_stress)
        assert len(statuses) == total_requests
        assert all(s == 502 for s in statuses)


@pytest.mark.trio
async def test_pyreqwest_stress_abrupt_disconnect_avalanche(
    server_ssl_context: ssl.SSLContext,
) -> None:
    """Avalanche of 150 abrupt client disconnects to stress proxy cancellation and cleanup."""
    churn_count = 150

    async with spawn_proxy_and_origin(server_ssl_context) as (proxy_port, dest_port):

        def run_asyncio_churn() -> None:
            async def amain() -> None:
                # Abruptly abort connections mid-stream using a 30ms timeout
                client = build_pyreqwest_client(
                    proxy_port, timeout_sec=0.03, max_conns=churn_count
                )

                async def fetch_and_drop(idx: int) -> None:
                    try:
                        req = client.get(
                            f"https://127.0.0.1:{dest_port}/slow_disconnect?id={idx}"
                        ).build()
                        await req.send()
                    except PyreqwestError:
                        pass

                tasks = [fetch_and_drop(i) for i in range(churn_count)]
                await asyncio.gather(*tasks)

            asyncio.run(amain())

        # Survived the avalanche
        await trio.to_thread.run_sync(run_asyncio_churn)

        # Confirm proxy is immediately healthy and responsive
        def run_asyncio_recovery_check() -> int:
            async def amain() -> int:
                client = build_pyreqwest_client(proxy_port, timeout_sec=5.0)
                req = client.get(f"https://127.0.0.1:{dest_port}/health").build()
                resp = await req.send()
                assert resp.status == 200
                return resp.status

            return asyncio.run(amain())

        recovery_status = await trio.to_thread.run_sync(run_asyncio_recovery_check)
        assert recovery_status == 200
