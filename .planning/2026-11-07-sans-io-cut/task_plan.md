# Task Plan: Sans-IO cut of the nano CONNECT proxy

## Goal
Measure how io-agnostic (sans-io) `trio_http_proxy` really is, then pull the genuinely sans-io logic out of the trio code so it has no trio dependency and tests without a runtime.

## Next Step
Done. Code committed as 477c491 and 97abdb1 on `nano` (not pushed). Only open follow-up: C10, add a per-test timeout (`pytest-timeout`, a new dependency and a `uv.lock` change); the user has not asked for it.

## Current Phase
Phase 6 (complete)

## Phases

### Phase 1: Requirements & Discovery
- [x] Ground rule from user: use LSP tools heavily (LSP rules apply to every subagent brief too)
- [x] Assess sans-io extent per module (see findings.md)
- [x] Document in findings.md
- **Status:** complete

### Phase 2: Planning & Structure
- [x] Decide the cut: parser, error-to-response mapping, timeout policy
- [x] Decide where the trio driver lives (proxy.py) and which API moves
- **Status:** complete

### Phase 3: Implementation
- [x] `protocol.py`: `ConnectParser`, `ConnectRequest`, `error_response`; no trio import
- [x] `timeouts.py` (new): `PipeTimeouts`
- [x] `proxy.py`: `process_as_connect_request` becomes a thin driver; single `ProxyProtocolError` handler; h11 import dropped
- [x] `streaming.py`: `pipe_with_timeout` uses `PipeTimeouts`
- [x] `__init__.py` exports; test imports; `AGENTS.md` architecture notes
- [x] `tests/test_sansio.py`: sync tests plus AST guard against trio imports
- **Status:** complete

### Phase 4: Testing & Verification
- [x] Baseline before edits: `pytest` 92 passed
- [x] After: `pyright` 0 errors, `ruff format`/`ruff check` clean, `pytest` 110 passed (final rerun after ruff reformat)
- **Status:** complete

### Phase 5: Delivery
- [x] Report result to user
- [x] LSP-diagnosis subagent report received (root cause: Claude Code client gitignore filter; no config change)
- [x] User approved the commit; code is in 477c491 (the cut) and 97abdb1 (review-gap tests) on `nano`, not pushed
- **Status:** complete

### Phase 6: Follow-up tranches (hierarchical delegation, pool_size 5)
User picked all four open items. Coordinator owns this file; workers report back only.
Pool accounting: LSP-diagnosis subagent finished. T1, T2, T3 finished and accepted; round 1 drained. Round 2: T4 (agent aad7db3ef94a6903f, general-purpose, no-spawn clause in brief) finished and accepted; pool drained = 0 of 5. Pre-dispatch manifest: scratchpad `manifest_round2_before.txt` (38 files, `.planning/` excluded).

| Tranche | Scope (only writes allowed) | Deps | Round |
|---------|-----------------------------|------|-------|
| T1 (agent a68fe73a191e04505, ACCEPTED) port fuzz tests to sync `ConnectParser` | `tests/test_fuzz_protocol.py` | none | 1 |
| T2 lazy package exports + runtime no-trio import test (agent a38bf32f1c34ae1aa) | `src/trio_http_proxy/__init__.py`, `tests/test_sansio.py` | none | 1 - ACCEPTED |
| T3 (agent ac43d08ea4b6b5808, ACCEPTED) forward `read_timeout`/`connect_timeout`/`maxreqsz`/`send_timeout` from `http_proxy` to `tunnel`; drop hard-coded `fail_after(30)` | `src/trio_http_proxy/proxy.py`, `docs/configuration.md`, `tests/test_tunnel_forwarding.py` (new) | none | 1 |
| T4 (agent aad7db3ef94a6903f, ACCEPTED) read-only review of uncommitted diff vs f87d9b9 | none (read-only; scratchpad only) | T1, T2, T3 accepted | 2 |

T4 waits because it reads `proxy.py`, which T3 writes. Scope-check manifest before round 1: scratchpad `manifest_round1_before.txt`.
- **Status:** complete (all four tranches accepted; final checks passed)

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| Move `process_as_connect_request` from `protocol.py` to `proxy.py` | `protocol.py` must not import trio; user said breaking changes on this branch are fine ("a little cowboy shit") |
| `ConnectRequest` is a `NamedTuple` | Unpacks and compares like the old `(host, port, trailing)` tuple, so callers and tests keep working |
| h11-style pull API: `receive_data()`, `receive_eof()`, `next_request()` | Matches the sans-io convention; `None` means need more bytes |
| `receive_data(b"")` is ignored; EOF is `receive_eof()` | h11 treats `b""` as EOF; a stray empty chunk must not end the stream |
| `next_request()` caches its result | Idempotent after success; h11 would otherwise return the next event |
| `error_response()` covers only 414 and 400 | 408/502/504 key on `trio.TooSlowError` and `OSError`, so they stay in `tunnel()` |
| `PipeTimeouts` in its own module `timeouts.py` | Keeps clock-free policy out of `streaming.py`, which imports trio |
| Drop `h11.ProtocolError` catches in `proxy.py` | Parser already wraps `h11.RemoteProtocolError`; no test referenced h11 |
| Guard trio-free modules with an AST test, plus a subprocess import with trio blocked | Superseded in round 1: T2 made `__init__.py` lazy, so a runtime check is now possible. The AST scan alone misses imports inside function bodies (T4 C6) |
| Lazy `__init__.py` exports (PEP 562) | Reversed the earlier "leave eager imports alone": the user picked it in round 1; package import no longer loads trio |
| Separate named plan instead of reusing an existing one | 16 other plans exist; avoids overwriting another task's state |

## Errors Encountered
| Error | Resolution |
|-------|------------|
| `LSP goToDefinition` returned "No definition found" on `trio.abc.HalfCloseableStream` (streaming.py:37, cols 42 and 44) | Root cause: Claude Code client drops goToDefinition/findReferences/goToImplementation/workspaceSymbol results in git-ignored paths (`.venv`). Not a pyright problem; no project config change helps. Workaround: `hover`, `incomingCalls`/`outgoingCalls`, or read source. Saved to memory |
| First `ruff format`/`ruff check` did nothing: zsh does not word-split `$F`, so one bogus path was passed | Reran with explicit file paths: 2 files reformatted, check clean |
| Malformed `Monitor` call (schema not loaded) | Not needed; background Bash notifies on completion |
| `echo =====` failed in zsh (`=` command-name expansion) while reading plan files | Used a different separator |
