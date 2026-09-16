# Findings & Decisions: Cashews Integration

## Requirements
- Rigorously refactor `trio_http_proxy` to cohesively integrate `cashews`.
- Maintain existing guarantees: structured concurrency, timeouts, connection limiters, error handling, clean logging.
- Maintain test coverage (all 67 tests passing + new comprehensive tests for the integrated features).
- Format (`ruff format`), lint (`ruff check --fix`), and typecheck (`ty check --fix` / `pyright`).

## Research Findings
1. **Existing Architecture**:
   - `trio_http_proxy` is a pure Trio-based asynchronous HTTP CONNECT proxy (`proxy.py`).
   - Handles `CONNECT host:port HTTP/1.1`, validates authority-form targets, connects via `trio.open_tcp_stream`, responds with `200 Connection established`, and pipes data bidirectionally using `trio.open_nursery()`.
   - Protection mechanisms in place: slowloris read timeout (`CONNECT_READ_TIMEOUT`), connect timeout (`DEST_CONNECT_TIMEOUT`), pipe idle timeout (`PIPE_IDLE_TIMEOUT`), pipe send timeout (`PIPE_SEND_TIMEOUT`), max concurrent connections limiter (`CONN_LIMITER`).
2. **Cashews Concurrency Model**:
   - `cashews` is hard-coded to standard Python `asyncio`:
     - `cashews.backends.memory.Memory.init()` calls `asyncio.create_task(...)`.
     - `cashews.backends.diskcache.DiskCache` calls `asyncio.get_running_loop().run_in_executor(...)`.
     - Lock and sleep primitives use `asyncio.Lock()`, `asyncio.sleep()`.
     - Directly awaiting `cashews` functions inside a `trio.run()` event loop raises `RuntimeError: no running event loop`.
3. **Investigation of `trio-asyncio` on Python 3.14**:
   - Tested `trio-asyncio 0.16.0` (installed via `uv add trio-asyncio`).
   - Fails immediately on import:
     `AttributeError: module 'asyncio.events' has no attribute 'BaseDefaultEventLoopPolicy'`
   - Root cause: In Python 3.14, `asyncio.events.BaseDefaultEventLoopPolicy` was removed from the standard library. `trio-asyncio` relies on this removed class and is incompatible with Python 3.14+.
   - Context7 lookup confirmed `trio-asyncio` is an older sub-project; modern interoperability uses guest runs or dedicated event loop bridges.
4. **Bridge Architecture (ProxyCache)**:
   - Running an asyncio event loop in a dedicated background worker thread (`ProxyCache`) bridges Trio and Cashews without conflicting with Trio's scheduler or depending on broken third-party loop patches.
   - Trio calls `trio.to_thread.run_sync(lambda: asyncio.run_coroutine_threadsafe(...).result())`.
   - Clean shutdown requires `await cache.close()` in the loop before stopping the loop, eliminating any pending background tasks (e.g. `Memory._remove_expired`).
5. **Proxy Features Cohesively Integrated**:
   - **DNS / Target Host Resolution Caching**:
     - `resolve_host(desthost)` wrapped in `@cache(ttl=DNS_CACHE_TTL, key="dns:{host}")`.
     - Resolves via thread executor `socket.getaddrinfo`, caching primary IP. Avoids repeating blocking DNS lookups on every connection.
   - **Destination Circuit Breaker**:
     - `@cache.circuit_breaker(key="cb:{host}:{port}", errors_rate=..., period=..., ttl=..., min_calls=...)`.
     - Pre-connection probe `check_circuit(host, port)`. If circuit is open, fast-fails immediately with HTTP 502 without waiting up to `DEST_CONNECT_TIMEOUT` (10s) or tying up sockets.
     - Connection outcome recorded via `record_connection_result(host, port, exc)`.
   - **Client Rate Limiting**:
     - `@cache.rate_limit(limit=..., period=..., ttl=..., key="rl:{key}")`.
     - Protects proxy from client connection floods, replying with `HTTP/1.1 429 Too Many Requests\r\nRetry-After: 60\r\n\r\n`.
   - **Pluggable Backends**:
     - Supports `mem://`, `disk://`, `redis://` via `CACHE_URL` env var.
   - **Lazy Lifecycle Management**:
     - `ProxyCache` lazily starts on first use; can also be managed via context manager (`with` / `async with`) and stopped explicitly in `start_server()`.

## Technical Decisions
| Decision | Rationale |
|----------|-----------|
| Modularize into `src/trio_http_proxy/cache.py` and re-export in `proxy.py` and `__init__.py` | Keeps code clean, separation of concerns between proxy protocol logic and cache/resilience infrastructure, while maintaining full backward compatibility. |
| Reject `trio-asyncio` in favor of `ProxyCache` worker-thread bridge | `trio-asyncio` crashes with `AttributeError` on Python 3.14 due to removed `BaseDefaultEventLoopPolicy`. The worker-thread bridge is pure standard library (`threading` + `asyncio`), robust, and 100% Python 3.14+ compatible. |
| Lazy thread startup for `DEFAULT_PROXY_CACHE` | Prevents starting background threads on import or in tests that don't need the cache. |
| Fallback gracefully if cache disabled | If `CACHE_ENABLED=false` or `CACHE_URL=""`, proxy runs without caching overhead. |
| Fast-fail circuit breaker with 502 | When destination is failing, immediate fail-fast prevents resource exhaustion and hung clients. |

## Issues Encountered
| Issue | Resolution |
|-------|------------|
| `RuntimeError: no running event loop` when running `cashews` directly in Trio | Implemented `AsyncioBridge` / `ProxyCache` running background thread event loop |
| `AttributeError: module 'asyncio.events' has no attribute 'BaseDefaultEventLoopPolicy'` when importing `trio-asyncio` | Avoid `trio-asyncio`; use standard library thread-bridged asyncio loop for `cashews` |
| `TypeError: A coroutine object is required` when calling `run_coroutine` on `cache.setup()` | `cache.setup()` is synchronous; only coroutine methods need `run_coroutine` |
| Pending task warning on shutdown | Await `cache.close()` before stopping asyncio event loop |
