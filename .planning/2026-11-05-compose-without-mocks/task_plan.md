# Task Plan: Compose without mocks

## Goal
Refactor effect boundaries so tests exercise composed production behavior without unittest.mock or patched application methods.

## Current Phase
Complete

## Next Step
None. Implementation and verification are complete.

## Phases

### Phase 1: Audit
- [x] Inspect mock usage and existing composition seams.
- [x] Establish baseline: 360 tests pass.
- **Status:** complete

### Phase 2: Refactor
- [x] Compose client identity and startup handoff effects explicitly.
- [x] Exercise real permits, cache recording, and backend recovery.
- [x] Remove unused socket substitutes and unittest.mock imports.
- [x] Replace DNS cache subclasses/no-op caches with injected resolvers and real caches.
- [x] Document dependencies, ownership, compatibility adapters, and scheduler behavior.
- **Status:** complete

### Phase 3: Verify
- [x] Run focused tests, full pytest, pyright, lint, and diff review.
- [x] Run coverage and repeat the concurrency-sensitive subset five times.
- [x] Confirm published LSP diagnostics are clean for changed production modules.
- **Status:** complete

## Decisions Made
- Keep Trio's MockClock: it is a virtual scheduler, not a replacement for application behavior.
- Retain environment isolation in settings tests; eliminate application-method/queue monkeypatching.
- Prefer real caches and breakers over interaction assertions; keep small contract adapters for legacy recorder compatibility.
- Pause virtual-clock autojump during real asyncio bridge I/O; enable it at the timed destination-connect boundary.
- Keep formatting changes scoped: the formatter flags pre-existing layout in proxy.py and test_builder.py, also reproducible from HEAD.

## Verification
- `just ci`: Ruff clean; Pyright 0 errors/warnings; 373 tests passed in 6.19s.
- Full coverage run: 373 passed in 6.49s; 90% overall statement coverage; 100% destination-breaker and permit statement coverage.
- Composition/recovery/lifecycle/CONNECT subset: 96 passed on each of five consecutive runs.
- LSP diagnostics: published and empty, with no indexing/degradation, for dependencies.py, proxy_cache.py, proxy.py, and config/builder.py.
- `git diff --check`: clean.
- Mock audit: no mocking-library imports or application monkeypatches remain in tests.

## Errors Encountered
| Error | Attempt | Resolution |
|-------|---------|------------|
| No .venv/bin/pyright | Initial baseline | `just typecheck` resolves installed pyright successfully. |
| Real TCP peer reset after unread request bytes | New rate-limit transport test | Send no request: rate limiting rejects the connection before CONNECT parsing. |
| Virtual clock expired an asyncio bridge submission before a connect attempt | Composing real DNS caching in overall timeout test | Disable autojump until the first destination attempt; then assert the full six-second connect budget. |
| Formatter flags existing source/test layout | Formatting check | Confirmed the same HEAD files fail; left unrelated formatting unchanged. |
