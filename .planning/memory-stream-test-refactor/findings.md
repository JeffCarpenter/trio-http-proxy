# Findings: refactoring tests onto trio.testing memory streams

Context: refactored `tests/test_proxy.py` to lean on `trio.testing.memory_stream_pair()`
and its hooks instead of `monkeypatch`-ing `trio.open_tcp_stream` / `proxy.pipe`, and
added a `_connect` dependency-injection parameter to `tunnel()`/`http_proxy()` in
`src/trio_http_proxy/proxy.py` (mirroring the existing `_nextid` pattern) to make that
possible. Commit: `2ce50f7`.

## Surprise 1: `except A, B, C:` is valid Python 3.14 syntax, not Python 2 rot

`proxy.py` had lines like:

```python
except AssertionError, ValueError, h11.ProtocolError, OSError:
```

This reads like leftover Python 2 syntax (`except Type, name:`) and was the first thing
that looked like a bug. It isn't: PEP 758 (Python 3.14) allows `except` to take multiple
exception types without wrapping them in parentheses. The project's `requires-python`
already pins 3.14+, so this is intentional, current syntax — confirmed by `ast.parse`
succeeding and `pyright`/`ruff` both being silent on it.

**Takeaway:** don't pattern-match "looks like old syntax" against a fixed mental model of
Python grammar without checking the project's actual `requires-python` floor first.

## Surprise 2: `receive_some_hook` / `send_all_hook` checkpoint twice before firing — a real race

`trio.testing.MemoryReceiveStream.receive_some()` does:

```python
await _core.checkpoint()
await _core.checkpoint()
if self.receive_some_hook is not None:
    await self.receive_some_hook()
```

So the hook doesn't fire synchronously on call — there are two scheduler checkpoints in
between. This mattered for
`test_http_proxy_mixed_exception_group_logs_both_handlers`, which needs *two* pipe()
tasks to fail with *different* exception types at roughly the same time, so that trio's
`except*` delivers both to their respective handlers in one `ExceptionGroup`.

First attempt: give each direction's stream a hook that raises immediately. Result: only
one exception ever showed up in the output. What happened: once one task's hook raised,
trio's nursery began cancelling the sibling task — and if the sibling hadn't yet cleared
its own two checkpoints, it got cancelled instead of reaching its hook and raising its
intended exception. This is a genuine scheduling race, not a flake in the test harness.

Fix: gate both hooks on a `trio.Event`, and only `.set()` it after confirming (via
`trio.testing.wait_all_tasks_blocked()`) that both pipe tasks are already parked waiting
on the event. Then both resume and raise in the same scheduler tick, before either one's
nursery-driven cancellation can preempt the other — because after `await release.wait()`
returns, the hook body has no further checkpoint before it raises.

**Takeaway:** synthetic unconditional-raise stubs (the old `monkeypatch.setattr(proxy,
"pipe", ...)` approach) don't have this problem because they never checkpoint before
raising. Swapping to real memory-stream hooks buys fidelity (the real `pipe()` code runs)
but reintroduces real trio scheduling races that the stub was accidentally hiding. Any
test asserting on *simultaneous* multi-task failure needs explicit synchronization
(`wait_all_tasks_blocked()` + a gate), not just "set two hooks and hope".

## Surprise 3: two of the rewritten tests were vacuous — and the passing gate didn't catch it

Verified late (thanks to advisor's second pass, not caught by my own test runs) via
fault injection — deliberately breaking what a test claims to check and confirming it
then fails:

- `test_process_as_connect_request_chunked_across_multiple_reads` asserted only
  `(host, port) == ("example.com", 443)`. Replacing the 3-byte trickle loop with a
  single `send_all(req)` still passed, because `process_as_connect_request`'s result is
  identical whether the request arrived in one `receive_some()` call or several — the
  test's name promised multi-read behavior it never actually checked. Fixed by counting
  `receive_some()` calls via a `receive_some_hook` and asserting `read_count > 1`.

- `test_pipe_forwards_multiple_chunks_in_order` asserted `received == b"firstsecond"` —
  concatenated bytes can't distinguish two forwarded chunks from one coalesced chunk.
  Fixed by asserting on `pipe()`'s own `"Forwarded N bytes"` log lines and their order.

Two other rewritten tests (`..._one_pipe_task_breaks`, `..._swallows_unexpected_exception`)
were checked the same way and turned out *not* vacuous — disabling their hook made them
hang (`timeout` killed them) rather than pass, confirming the assertions really do depend
on the hook firing.

**Takeaway:** "all tests pass" and "coverage % unchanged" are necessary but not
sufficient checks for a test refactor. The only way to know a test still tests anything
is to break the thing it names and confirm it fails. Green tests after a refactor can
mean the refactor is fine, or that a test quietly stopped checking anything, and nothing
in a normal test run distinguishes the two cases.

## Smaller notes

- `close_hook` on `MemorySendStream`/`MemoryReceiveStream` is a *sync* callable, while
  `send_all_hook`/`receive_some_hook`/`wait_send_all_might_not_block_hook` are *async*.
  Mixing these up fails immediately and loudly (calling a sync function as a coroutine,
  or awaiting a non-awaitable), so this is a fast, not a subtle, mistake — but worth
  knowing going in rather than discovering via a traceback.
- `trio.StapledStream.aclose()` closes `send_stream` first, then `receive_stream` in a
  `finally`, and re-raises whatever `send_stream.aclose()` raised. That's what makes a
  `close_hook` on the send side a clean, minimal replacement for the old
  `_RaisesOnClose` wrapper class that overrode `aclose()` entirely.
