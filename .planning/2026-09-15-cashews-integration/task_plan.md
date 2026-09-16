# Task Plan: Rigorously Integrate Cashews into trio_http_proxy

## Goal
Cohesively integrate `cashews` into `trio_http_proxy` to provide robust caching, DNS resolution caching, circuit breaking for failing destination hosts, and rate limiting while seamlessly bridging Trio and asyncio.

## Next Step
Deliver summary of completed work to user.

## Current Phase
Phase 5

## Phases

### Phase 1: Requirements & Discovery
- [x] Analyze codebase, tests, and pyproject.toml
- [x] Test `cashews` in Python / Trio environment
- [x] Discover asyncio dependency of `cashews` (requires running asyncio loop)
- [x] Test Trio <-> asyncio bridge options
- **Status:** complete

### Phase 2: Architectural Design
- [x] Design seamless background asyncio loop bridge for Trio
- [x] Design DNS / Target Host resolution caching with `cashews`
- [x] Design Destination Circuit Breaker with `cashews.circuit_breaker`
- [x] Design Request / Host Rate Limiter with `cashews.rate_limit`
- [x] Design configuration (environment variables & programmatic options)
- **Status:** complete

### Phase 3: Implementation
- [x] Implement Trio-asyncio cashews bridge runner (`ProxyCache` in `cache.py`)
- [x] Implement cached destination resolution and circuit breaker in proxy pipeline
- [x] Integrate configurable rate limiting and cache backends (`mem://`, `disk://`, etc.)
- [x] Ensure graceful startup and shutdown of cache and background loop
- [x] Update `proxy.py` and `__init__.py` exports
- **Status:** complete

### Phase 4: Testing & Verification
- [x] Add comprehensive test suite covering cashews caching, circuit breaker, rate limiter, and error paths (`tests/test_cache.py`)
- [x] Verify all existing 67 tests pass without regression
- [x] Run `ruff check --fix`, `ruff format`, `ty check --fix`, and `pyright`
- [x] Leverage `mcpls` tools (`get_document_symbols`, `get_diagnostics`, `get_hover`, `get_definition`, `get_references`, `prepare_call_hierarchy`, `workspace_symbol_search`)
- **Status:** complete

### Phase 5: Documentation & Delivery
- [x] Update CLAUDE.md documenting cashews integration and configuration
- [x] Document findings and progress in `.planning/`
- [x] Deliver summary of changes
- **Status:** complete

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| Isolate cashews in an asyncio worker thread bridge (`ProxyCache`) | Cashews relies directly on `asyncio` primitives (`asyncio.create_task`, `asyncio.sleep`, `asyncio.Lock`). Trio cannot run these directly. Running an asyncio event loop in a dedicated background thread bridged via `trio.to_thread.run_sync` provides robust, clean, zero-external-dependency integration. |
| Reject `trio-asyncio` on Python 3.14 | `trio-asyncio 0.16.0` crashes on import on Python 3.14 due to removed `BaseDefaultEventLoopPolicy`. Standard library thread bridge is fully compatible. |
| Modularize into `cache.py` and re-export in `proxy.py` and `__init__.py` | Keeps code modular and maintainable while maintaining 100% backward compatibility. |
| Lazy thread startup for `DEFAULT_PROXY_CACHE` | Prevents starting background threads on import or in tests that don't need the cache. |
| Fast-fail circuit breaker with 502 | When destination is failing, immediate fail-fast prevents resource exhaustion and hung clients. |
| MockClock auto-jump detection | `ProxyCache` detects Trio's `MockClock(autojump_threshold=0)` to prevent test time jumping past deadlines during mock clock tests. |

## Errors Encountered
| Error | Resolution |
|-------|------------|
| `RuntimeError: no running event loop` when running `cashews` directly in Trio | Bridge cashews async calls through an asyncio loop running in a dedicated worker thread |
| `AttributeError: module 'asyncio.events' has no attribute 'BaseDefaultEventLoopPolicy'` when importing `trio-asyncio` | Avoid `trio-asyncio`; use standard library thread-bridged asyncio loop for `cashews` |
| `TypeError: A coroutine object is required` when calling `run_coroutine` on `cache.setup()` | `cache.setup()` is synchronous; only coroutine methods need `run_coroutine` |
| Pending task warning on shutdown | Await `cache.close()` before stopping asyncio event loop |
