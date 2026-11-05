# Findings

- Baseline commit: c78fead; working tree initially clean. Full baseline CI passes (373 tests).
- Existing HTTPS stress tests cover throughput, quotas, storms, and disconnects, but primarily compose narrow cache adapters; they do not establish real-cache bridge cleanup for all these scenarios.
- Existing lifecycle tests cover single cancellation/startup races and four concurrent synchronous stop callers. Extend these with batches of pending real backend operations and concurrent asynchronous close callers.
- `ProxyCache.run` propagates Trio cancellation to its concurrent Future; shutdown cancels bridge tasks, gathers them, closes backend once, shuts down executor, and closes the loop.
- Loopback tests should explicitly assert tracked accepted streams/listeners close, handler counts balance, limiter tokens return, and owned bridge threads/loops terminate. Global thread/task counts would include unrelated test infrastructure.
- Branch coverage is not currently requested by the default CI recipe; collect a full-suite branch report separately rather than treating statement coverage as branch coverage.
- Running Cashews owns a memory-expiration task. Compare task identities to the startup baseline, then assert those tasks finish at shutdown; an empty live-loop assertion would incorrectly reject healthy production behavior.

## Branch coverage (full 385-test suite)

Measured with pytest-cov `--cov-branch` on Python 3.14.9. Reports:
- JSON: `/tmp/trio-http-proxy-branch-coverage.json`
- HTML: `/tmp/trio-http-proxy-branch-coverage/index.html`

| Scope | Covered branches | Branch coverage |
| --- | ---: | ---: |
| Entire package | 385 / 492 | 78.25% |
| `proxy_cache.py` | 121 / 138 | 87.68% |
| `proxy.py` | 79 / 100 | 79.00% |
| Destination breaker | 46 / 46 | 100.00% |
| Circuit permit | 4 / 4 | 100.00% |
| Circuit-breaker manager | 10 / 20 | 50.00% |
| Settings builder | 8 / 46 | 17.39% |

Statement coverage is 1641 / 1826 = 89.87%; combined statement/branch coverage is 87.40%. There are 107 missing branches, including 59 partially covered branch points. No exclusions were added.

## Remaining gaps

- `config/builder.py`: optional cache/circuit-breaker arguments, fluent setters, and settings-builder contexts; 38 missing branches, the largest single gap. This is SettingsBuilder, not ProxyCacheBuilder.
- `circuit_breaker_manager.py:104-124`: eviction under capacity pressure when no empty idle CLOSED breaker is available; fallback selection among populated CLOSED, HALF_OPEN, and OPEN entries.
- `proxy_cache.py:422-475`: rare loop-creation/startup-abort/error-publication paths and a late-publication timeout branch; also defensive submission/shutdown errors and rate-limit TTL repair/cache-clear failure paths.
- `proxy.py`: CLI startup/help, alternate context/logging/handler branches, and exceptional transport paths. Complete breaker/permit branch coverage does not imply complete manager or proxy coverage.

These gaps were reported rather than filled with artificial method replacements or coverage-only assertions. The requested new tests exercise real composition and resource ownership; they exposed no production defect.
