# Testing with injected dependencies

`ProxyCache` and `ProxyCacheBuilder` accept four optional, keyword-only dependencies:

| Argument | Contract | Default |
| --- | --- | --- |
| `backend` | `CacheBackend`: setup, health, key/value, counter, expiration, and cleanup operations | A new Cashews `Cache` per instance |
| `resolver` | `HostResolver`: blocking `host -> Sequence[str]` | System TCP address lookup |
| `clock` | Callable returning monotonic seconds | `time.monotonic` |
| `thread_factory` | `ThreadFactory`: creates a `ThreadHandle` with start/join/is_alive | `threading.Thread` |

The protocols are exported from `trio_http_proxy` and `trio_http_proxy.cache`.
They use structural typing: collaborators need not inherit from a particular
implementation or base class. Dependencies are runtime objects, not settings or
environment variables. Existing callers need no changes.

For example, test DNS caching without changing global socket functions:

```python
from trio_http_proxy import ProxyCacheBuilder

lookups = []


def resolve(host: str) -> list[str]:
    lookups.append(host)
    return ["192.0.2.10", "192.0.2.10", "2001:db8::10"]


async def check_dns() -> None:
    builder = ProxyCacheBuilder(resolver=resolve).with_dns_cache("60s")
    async with builder as cache:
        assert await cache.resolve_hosts("example.test") == [
            "192.0.2.10", "2001:db8::10"
        ]
        await cache.resolve_hosts("example.test")
        assert lookups == ["example.test"]
```

## Ownership and execution

- A cache owns its backend's setup and close, even when the backend is injected.
  Do not share one backend across live cache instances or overlapping builder
  contexts. A builder without an injected backend creates fresh backends as usual.
- `setup` runs synchronously during startup. Async backend methods run on the
  cache's dedicated asyncio bridge loop. A test backend must follow the same
  execution contract; it must not call Trio APIs from that loop.
- The resolver runs in the bridge loop's executor, not on Trio's thread. It may
  return duplicate addresses; caching removes duplicates in first-seen order.
  Empty results and resolver `OSError`s remain DNS failures, not backend outages.
  IP literals bypass resolution, and disabled DNS caching preserves the existing
  raw-host fallback behavior.
- The injected clock controls only backend degradation/recovery probe scheduling.
  It does not change Cashews TTLs, circuit-breaker time, or Trio's clock. Advance
  a fake clock and await an operation to exercise recovery without sleeping.
- The thread factory receives `target`, `daemon`, and `name` as keyword arguments.
  A supplied handle must honor normal thread startup, liveness, and join semantics.
  DNS executor workers are not created through this factory.

`tests/cache_fakes.py` provides a controllable, protocol-compatible backend. Its
instance-local effects model setup, operation, and ping failures and recovery
without replacing collaborator methods. Environment-variable tests still use
`monkeypatch.setenv`/`delenv` because environment loading is the behavior under test.

## Property tests

`tests/test_cache_dependencies.py` uses Hypothesis to generate DNS address lists,
TTLs, and recovery-clock histories. It checks stable address deduplication, cache
hits and namespace clearing, and probe throttling/recovery at interval boundaries.
Each example owns fresh collaborators and cleans up its bridge thread.

## CI and pre-commit checks

Install `just`, `uv`, `ruff`, and `pyright` on your PATH, then run:

```bash
just ci
```

The `justfile` runs `ruff check`, Pyright, and the full test suite, including
integration and Hypothesis tests, stopping on the first failure. Pyright and pytest
use `uv run --locked` to prepare the project environment without changing the
lockfile. Run checks separately with `just lint-ci`, `just typecheck`, or `just test`.

For local fixes, install `ty` on your PATH and run `just check`. It runs
`just typecheck-ty` (`ty check --fix`) followed by `just lint` (`ruff check --fix`),
stopping if either fails. Unlike `just ci`, these recipes may modify files.
