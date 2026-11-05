# Findings

- Baseline: 287 tests, all passing.
- Collaborator monkeypatches were confined to cache setup/ping/get/incr, socket.getaddrinfo, time.monotonic, and threading.Thread.
- Existing circuit-breaker APIs already accept explicit time; proxy networking already has injection seams.
- One shutdown test directly overwrote loop.shutdown_asyncgens; replaced with observable async-generator finalization.
- One proxy fallback-cache test assigned a method dynamically; replaced with a local fake class.
- Default Cashews cache satisfies CacheBackend without inheritance/adapters; ControlledBackend composes a protocol-typed backend.
- DNS uses the same namespace:dns:host key, TTL seconds, stable deduplication, executor scheduling, and error distinction after removing decorators.
- Explicit environment tests retain setenv/delenv. No collaborator setattr remains.
- mcpls diagnostic cache initially could not resolve a newly created module; command-line pyright was clean. Python server restarted to refresh imports; final published diagnostics are clean.
- Final verification: 301 tests pass, pyright clean, scoped Ruff clean.
- Two Hypothesis properties each run 40 examples; DNS coverage includes explicit expiration and generated TTL checks without wall-clock sleeps.
- Thread creation/admission errors now leave STOPPED instead of stranding STARTING; both paths have regression tests.
