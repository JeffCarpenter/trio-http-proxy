# Findings & Decisions

## Requirements
- Rigorously refactor through composition so tests do not require mocks.
- Preserve CONNECT, cache recovery, breaker, and lifecycle behavior.

## Research Findings
- Existing backend, resolver, clock, and thread protocols already expose most effects.
- unittest.mock is used only in simplification contracts and probe throttling.
- Builder direct recording test calls a local fake and never exercises production recording.
- Broken-resource rate-limit reproduction never reaches rate limiting: memory streams have no socket identity.
- Startup deadline reproduction patches queue.Queue globally; handoff should instead be composed locally.
- Unused MockSocket/StreamWithSocket helpers can be deleted; client-key extraction can be tested using a real socket pair.

## Technical Decisions
- Add client_key_resolver callable to http_proxy (default remains socket-based extraction). Trusted server composition controls this identity, never CONNECT input.
- Add StartupChannel protocol/factory for thread publication/deadline I/O; default remains a bounded queue.
- Test permit finalization against real breaker state and subsequent admission.
- Record controlled backend operation names for deterministic probe throttle assertions.
- Forward startup_channel_factory through the fluent builder and export StartupChannel/StartupResult alongside existing dependency contracts.
- Test client identity with a real TCP peer and custom memory-stream identities; retain None as the opt-out from per-client limiting.
- Retain small sync/async/legacy recorder adapters only where the adapter contract itself is under test.
- Disable scheduler autojump during real asyncio bridge submissions; destination timeout tests start virtual timing at the connect effect boundary.

## Issues Encountered
- Baseline pytest: 360 passed in 10.17s.
- pyright lives on PATH, not in .venv; `just ci` resolves it successfully.
- A TCP server closing before reading queued request bytes can reset the peer; the real-socket rate-limit test sends no request because admission precedes parsing.
- Trio autojump can expire the bridge submission timeout before asyncio's real worker runs. The composed overall-connect test now separates those scheduling domains.

## Final Evidence
- 373 passing tests (baseline 360), Ruff clean, Pyright 0 errors/warnings.
- 90% overall statement coverage; destination breaker and permit modules at 100%.
- 96 concurrency-sensitive tests passed five consecutive runs.
- No unittest.mock imports, mock cache/socket substitutes, application-method patches, or ProxyCache test subclasses remain.
- Trio MockClock and settings environment isolation intentionally remain.

## Resources
- src/trio_http_proxy/dependencies.py
- src/trio_http_proxy/proxy_cache.py
- src/trio_http_proxy/proxy.py
- tests/cache_fakes.py
