# Progress

## Baseline
- pytest: 287 passed in 6.36s.

## Implementation
- Added public structural CacheBackend, HostResolver, ThreadHandle, ThreadFactory protocols.
- Added optional backend/resolver/clock/thread_factory constructor parameters and builder forwarding.
- Replaced Cashews DNS decorator dependency with public get/set operations, preserving namespace keys, TTL, executor use, ordered deduplication, and error distinction.
- Migrated startup, recovery, DNS failure, executor shutdown, and thread-startup timeout tests.
- Removed reflective probe dispatch and real sleeps from recovery tests.
- Added Hypothesis as a locked development dependency.
- Added 14 regression/property tests: dependencies and builder contexts, isolation, DNS order/deduplication/TTL/expiration/clearing, resolver errors, literal bypass, disabled features, generated probe scheduling, and thread-admission failures.
- Added docs/testing.md with defaults, ownership, threading, fake clock scope, and examples.
- Remaining monkeypatches only configure the environment under test.

## Final verification
- Full pytest: 301 passed in 6.48s (80 Hypothesis examples across two property tests).
- Pre-commit verification repeated on user request: pyright clean, 301 tests passed in 6.46s, git diff --check clean.
- pyright: 0 errors, 0 warnings, 0 informations.
- mcpls: published, non-indexing, empty diagnostics for proxy_cache.py, dependencies.py, builder.py, cache_fakes.py, test_cache_dependencies.py.
- mcpls hover/reference checks confirmed public typed constructor and builder forwarding.
- Scoped Ruff lint and formatting: clean; git diff --check: clean.
- Diff reviewed; uv.lock adds Hypothesis and its existing sortedcontainers dependency reference, without unrelated upgrades.

## Outcome / limits
Complete. No external-network manual smoke test was run; the complete existing suite (including proxy integration tests) passes. Backend ownership and clock/thread contracts are documented. User authorized committing the completed refactor after final verification.
