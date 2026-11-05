# Task Plan: Minimal Trio HTTP Proxy

## Goal
Extract a minimal HTTP CONNECT proxy to git branch `mini`. Build strictly on Trio structured concurrency and RFC 7231. Remove Cashews caching, the background asyncio thread, circuit breakers, rate limiters, and Pydantic settings. See [meta.md](meta.md) for architectural invariants and scope specifications.

## Current Phase
Complete (All Quality Gates Passed)

## Phase Checklist
- [x] Phase 0: Setup
  - [x] Check out branch `mini`.
  - [x] Create planning files (`task_plan.md`, `findings.md`, `progress.md`, and `meta.md`).
  - [x] Untrack and ignore `.planning/.active_plan`.
  - [x] Record architectural invariants and scope in `meta.md`.
- [x] Phase 1: Design and Pure Core Extraction
  - [x] Write `src/trio_http_proxy/config.py`: frozen `ProxyConfig` with environment loader.
  - [x] Write `src/trio_http_proxy/errors.py`: `ProxyError`, `ProxyProtocolError`, and `RequestTooLargeError`.
  - [x] Write `src/trio_http_proxy/http.py`: wire-format byte responses (200, 400, 408, 414, 502, 504).
  - [x] Write `src/trio_http_proxy/protocol.py`: pure RFC 7231 §4.3.6 target parser and incremental `h11` stream consumer.
  - [x] Write `tests/test_config.py` and `tests/test_protocol.py`.
- [x] Phase 2: Structured Concurrency Core Implementation
  - [x] Write `src/trio_http_proxy/streaming.py`: timeout-oblivious `pipe()` with `send_eof()`, and `pipe_with_timeout()` with `CancelScope` nudging.
  - [x] Write `src/trio_http_proxy/proxy.py`: `tunnel()`, `http_proxy()` with nursery and `CapacityLimiter`, and `start_server()`.
- [x] Phase 3: Packaging and Dependency Pruning
  - [x] Delete legacy modules (`cache/`, `circuit_breaker/`, `connection/`, `proxy_cache.py`, `dependencies.py`, `util.py`).
  - [x] Update `src/trio_http_proxy/__init__.py` and `src/trio_http_proxy/__main__.py`.
  - [x] Remove `cashews`, `pydantic-settings`, and `trio-asyncio` from `pyproject.toml`.
  - [x] Run `uv sync` to refresh lockfile.
  - [x] Delete legacy tests in `tests/` and clean `conftest.py`.
- [x] Phase 4: Verification Suite
  - [x] Stream A (T1): Rewrite `tests/test_proxy.py` (memory streams, status codes 200, 400, 408, 414, 502, 504, chunk boundaries).
  - [x] Stream B (T2): Write `tests/test_invariants.py` (half-close, idle/send timeouts, sibling cancellation).
  - [x] Stream C (T3): Update `tests/test_proxy_tcp_integration.py` (live loopback TCP sockets, CapacityLimiter).
- [x] Phase 5: Quality Gates & Documentation
  - [x] Stream D (T4): Update `README.md`, `CLAUDE.md`, and `docs/configuration.md`.
  - [x] Merge all worktrees conflict-free into `mini`.
  - [x] Pass `mcpls` diagnostics: 0 errors across all files.
  - [x] Pass `ruff check`: All checks passed.
  - [x] Pass `ruff format --check`: All files formatted.
  - [x] Pass `pyright`: 0 errors, 0 warnings, 0 informations.
  - [x] Pass `ty check`: All checks passed.
  - [x] Pass `pytest`: 58/58 tests passed in 0.72s.
  - [x] Live `curl` smoke test: verified HTTPS tunnel through local proxy.

## Decisions Made
- Checked out dedicated branch `mini` from `main`.
- Moved architecture and scope specification to `meta.md`.
- Enforce strict nursery scoping (`trio.open_nursery`): no task outlives its caller.
- Support bidirectional TCP half-close via `send_eof()`.
- Guard against Slowloris reads and stalled send buffers by nudging cancel scope deadlines.
- Use frozen dataclass for configuration; avoid mutable module state and Pydantic models.
- Executed parallel streams using isolated `git worktree`s under `hierarchical-delegation`.
- Supervised and reviewed streams using `pi` via `hcom`.
- Untracked and ignored `.planning/.active_plan`.

## Errors Encountered
- None.
