# Research and Architectural Decisions: Minimal Trio HTTP Proxy

## 1. Scope
The `mini` branch extracts a minimal HTTP CONNECT proxy from `main`.

The `main` branch accumulated non-core mechanisms:
- Cashews cache bridge running an asyncio event loop inside an OS thread.
- Circuit breaker state machine tracking sliding windows, lease epochs, and permits.
- Rate limiters and client IP tracking.
- Pydantic settings hierarchy with 30 forwarding properties.
- ContextVars for mutable stream context.

The `mini` branch removes these mechanisms. It retains only the HTTP CONNECT tunnel.

## 2. Structured Concurrency Invariants
Trio requires strict concurrency boundaries:
1. **Bounded Lifetimes:** Every task runs inside a nursery (`async with trio.open_nursery() as nursery:`). When a function returns, all child tasks have completed. No tasks or threads run in the background.
2. **Half-Close Support:** TCP tunnels are full-duplex. When one side finishes writing (`from_stream.receive_some() == b""`), the forwarder calls `await to_stream.send_eof()`. The other direction continues transferring data until it reaches EOF.
3. **Cooperative Cancellation and Timeouts:** Cancel scopes wrap all I/O. Idle and send timeouts nudge cancel scope deadlines on progress (`on_receive`, `on_send`). When one direction fails or times out, the nursery cancels the other direction immediately.
4. **Guaranteed Resource Cleanup:** Streams close via `async with` blocks or `try...finally: await stream.aclose()`. No file descriptor or socket leaks on normal exit, cancellation, or error.
5. **Backpressure:** A shared `trio.CapacityLimiter` bounds total concurrent connections.

## 3. Wire Protocol and Responses
- Pure protocol parsing using `h11` for the CONNECT request line.
- Validation of RFC 7231 §4.3.6 authority-form (`host:port`), including bracketed IPv6 literals.
- Responses:
  - `200 Connection established`: tunnel opened.
  - `400 Bad Request`: malformed request or target not in authority-form.
  - `408 Request Timeout`: slow request headers.
  - `414 URI Too Long`: request exceeds configured byte ceiling.
  - `502 Bad Gateway`: destination host unreachable or connection refused.
  - `504 Gateway Timeout`: destination connection timed out.

## 4. File Layout
- `src/trio_http_proxy/config.py`: Frozen dataclass `ProxyConfig` with environment loader.
- `src/trio_http_proxy/errors.py`: Exception hierarchy (`ProxyError`, `ProxyProtocolError`, `RequestTooLargeError`).
- `src/trio_http_proxy/http.py`: Wire-format HTTP response byte constants.
- `src/trio_http_proxy/protocol.py`: Pure target parser and incremental `h11` stream consumer.
- `src/trio_http_proxy/streaming.py`: Timeout-oblivious `pipe()` with `send_eof()` support, and `pipe_with_timeout()`.
- `src/trio_http_proxy/proxy.py`: `tunnel()`, `http_proxy()`, and `start_server()`.
- `src/trio_http_proxy/__init__.py`: Clean public exports.
- `src/trio_http_proxy/__main__.py`: CLI entry point.
- `src/trio_http_proxy/py.typed`: Type marker.

Removals:
- Delete `cache/`, `circuit_breaker/`, `connection/`, `dependencies.py`, and `proxy_cache.py`.
- Remove `cashews`, `pydantic-settings`, and `trio-asyncio` from `pyproject.toml`.
