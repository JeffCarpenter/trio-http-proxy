# Findings & Decisions

## Requirements
- Find out to what extent the code is io-agnostic / truly sans-io.
- Implement "the truly sans-io cut".
- Ground rule: use LSP tools heavily; fall back to grep only for non-symbol text or when no server answers, and say so.
- User is fine with breaking changes and a little "cowboy" on the `nano` branch.

## Research Findings
Pre-change line refs, against commit f87d9b9.

- Truly sans-io before the change: `parse_connect_target` (`protocol.py:17`, stdlib `urlsplit` only), `http.py`, `errors.py`.
- `process_as_connect_request` (`protocol.py:59`) was sans-io logic welded into a trio loop. Its only trio uses (found with `findReferences` on the `trio` import): annotation `:60`, `trio.fail_after` `:80`, and `await stream.receive_some` `:94`, the only I/O call.
- `pipe` (`streaming.py:13`) is stream-type agnostic only through `trio.abc` ABCs. It still uses `trio.BrokenResourceError`/`ClosedResourceError` (`:40`) and a nominal `isinstance(to_stream, trio.abc.HalfCloseableStream)` (`:37`).
- That `isinstance` is nominal: `trio/_abc.py:243,489` are plain `ABC` classes with no `__subclasshook__`. A duck-typed stream silently loses half-close.
- `pipe_with_timeout` was welded to `CancelScope`, `trio.current_time` and `TooSlowError`; its idle/send phase and deadline policy was pure state.
- `tunnel`/`http_proxy`/`start_server` are inherently trio: `fail_after`, `aclose_forcefully`, nurseries, `except*` on trio errors, `serve_tcp`. Only seam is the private `_connect` kwarg, typed to return `trio.abc.Stream`.
- `proxy.py` imported h11 only for `except h11.ProtocolError` (`:46`, `:89`), per `findReferences` on the h11 import.
- Nothing was importable without trio: `protocol.py:9` imported it, and `__init__.py` imports `proxy` and `streaming` eagerly. After the cut this was still true for the package; round 1 (T2) made the exports lazy, so it no longer is.
- Tests paid for the welding: `ChunkedReceiveStream` (`tests/test_fuzz_protocol.py:24`) existed only to feed bytes to the parser.
- A byte tunnel has no protocol state beyond the handshake, so the sans-io surface is small: handshake parser plus timeout policy.
- Stale `cache/`, `circuit_breaker/`, `connection/` directories under `src/trio_http_proxy/` hold only untracked `__pycache__`.

### After the cut (current tree, uncommitted)
- `protocol.py`, `timeouts.py`, `errors.py`, `http.py` import no trio (enforced by `tests/test_sansio.py`).
- `proxy.py` has no h11 import.
- Public path change: `trio_http_proxy.protocol.process_as_connect_request` is gone; `trio_http_proxy.process_as_connect_request` and `trio_http_proxy.proxy.process_as_connect_request` work.

## Technical Decisions
| Decision | Rationale |
|----------|-----------|
| See "Decisions Made" in task_plan.md | Single source for the decision list |

## Issues Encountered
| Issue | Resolution |
|-------|------------|
| LSP `goToDefinition` fails on `trio.abc.HalfCloseableStream`, and on every symbol defined under `.venv` (`trio.abc.ReceiveStream`, `trio.TooSlowError`, `trio.CancelScope`, `import trio`) | Root cause: the Claude Code client runs `git check-ignore` on result URIs and drops ignored ones, only for `findReferences`, `goToDefinition`, `goToImplementation`, `workspaceSymbol`. `.venv` is ignored (`.gitignore:86`). Pyright itself returns correct locations when probed directly. `hover` and call hierarchy are not filtered. No config change fixes it; nothing was changed |
| Diagnostics from `lsp_probe.py`, `use2.py` and `trio/_abc.py` appeared mid-session | They came from the diagnosing subagent's probe scripts under the scratchpad; none are in the repo (`git status` clean of strays) |
| Subagent briefly edited shared `pyrightconfig.json` for an experiment while other workers were running | Restored; sha256 equals HEAD (checked). Its tool-side test of moving trio outside the repo was inconclusive |

## Open items noticed, not acted on
- Resolved in round 1: `http_proxy` now forwards `read_timeout`, `connect_timeout`, `maxreqsz` and `send_timeout` to `tunnel`; the hard-coded `fail_after(30)` is gone; exports are lazy.
- 408/502/504 mapping is still inline in `tunnel()`.
- `pyrightconfig.json` shadows `[tool.pyright]` in `pyproject.toml`, so `include = ["src"]` there is dead and `uv run --locked pyright` also checks `tests/`. Two configs also drift (the JSON adds `pythonVersion` and a hard-coded `.venv/lib/python3.14/site-packages` in `extraPaths`).

## Resources
- `AGENTS.md` (architecture and safety invariants, updated for the new modules)
- `docs/configuration.md` (names `process_as_connect_request(...)` params; still accurate)
- Test and verification output files are under the session scratchpad directory (`base_pytest.txt`, `after.txt`, `ruff.txt`, `final.txt`)

## T4 independent review (round 2, agent aad7db3ef94a6903f)
The reviewer found no defect in `src/`. Differential runs against the old tree `f87d9b9` (handshake about 72,000 cases, pipe about 37,000, tunnel mapping, h11 leak fuzz 240,000) showed 0 differences. The reviewer's own words were in 文言 and are condensed here. Full report: the agent output file for task aad7db3ef94a6903f; its scripts are in the scratchpad `t4/` directory.

Coordinator reproduced C1 (below) and later checked the fixes by mutation.

Test gaps caused by this change (fixed by the coordinator, each killed by a mutation check):
- C1: `process_as_connect_request` EOF branch (`proxy.py`) had no trio-level test after T1 made the fuzz tests sync. Mutant `await trio.sleep_forever()` instead of `parser.receive_eof()` passed all 144 tests. Fix: `test_process_as_connect_request_eof_is_protocol_error_without_waiting` in `tests/test_fuzz_protocol.py`; the mutant now fails 3 of 3 cases.
- C4: nothing pinned the shared handshake deadline that `docs/configuration.md` promises. Fix: `test_handshake_sends_share_one_send_timeout` in `tests/test_tunnel_forwarding.py`; the separate-deadlines mutant now fails only that test.
- C7: dropping `"timeouts"` from `_SUBMODULES` passed all tests, because in-process tests had already imported the submodule. Fix: `test_submodule_attribute_resolves_in_a_fresh_interpreter` in `tests/test_sansio.py`; the mutant fails only the `timeouts` case.
- C8: `assert name in dir(trio_http_proxy)` after `getattr` could not fail (getattr caches into `globals()`). Removed.
- Doc fixes: `AGENTS.md` now says what the sans-io guard checks and what it misses (C6, B2), and that `__init__.py` is lazy with three places to edit (B1); `process_as_connect_request` docstring names `ConnectRequest` (B3).

Pre-existing gaps. C2, C3 and C5 were fixed later at the user's request (commit 97abdb1, mutation-checked). C10 is still open:
- C2: `protocol.py` `_received >= maxreqsz` boundary (changing `>=` to `>` passes every test, old tree too).
- C3: `streaming.py` `cancel_scope.deadline = policy.received/sent(...)` never checked by time (two mutants survive, old tree too).
- C5: `http_proxy` forwarding `idle_timeout`/`send_timeout` to the pipes (swap and drop mutants survive).
- C10: no per-test timeout (no pytest-timeout), so seven mutants were caught only by a hang, not an assertion; CI would hang instead of fail.

Behaviour notes, not defects: A4 (the `h11.ProtocolError` catches are gone, so any non-`ProxyProtocolError` escaping the parser would reach `trio.serve_tcp`; none can today), A7 (`send_timeout` now also bounds the handshake writes, default still 30), A8 (the debug log `Reading CONNECT request...` moved from logger `trio_http_proxy.protocol` to `trio_http_proxy.proxy`), A9 (a `ConnectParser` error is not latched: a second `next_request()` after an error raises a different message; feeding after EOF raises `RuntimeError`; the driver never does either), A10 and A11 (harmless).

Method surprise: tests in `tests/test_sansio.py` that run `python -I` subprocesses ignore `PYTHONPATH`; they load the repo source through the `.pth` file. A `PYTHONPATH` scratch-copy mutation therefore does not affect those tests. For a subprocess-test mutation, change the live file with a backup and a sha256 restore check (done for C7).

## Lessons for worker briefs (reuse in every dispatch)
Tool traps
- LSP `goToDefinition`/`findReferences`/`goToImplementation`/`workspaceSymbol` return nothing for symbols under the gitignored `.venv`. Use `hover`, call hierarchy, `pydoc`, or read the source under `.venv`. The LSP index is also stale right after a file changes; say so when falling back to grep.
- `pyrightconfig.json` shadows `[tool.pyright]` in `pyproject.toml`; `uv run --locked pyright` also checks `tests/`. Never edit `pyrightconfig.json` during a round (an earlier subagent did, then restored it).
- zsh does not word-split `$F`; use explicit paths or `${=F}`. `echo =====` fails (`=` expansion). The working directory drifts, so start each Bash call with `cd /home/jeff/code/fork/trio_http_proxy`.
- `requires-python >= 3.14`: `except A, B:` without parentheses (`streaming.py:42`) is valid PEP 758 syntax.
- Library docs: Context7 first, `pydoc` over `inspect`, and the `.venv` source as the final word.

Method
- Mutation check on a scratch copy of `src/trio_http_proxy` with `PYTHONPATH`, after printing `trio_http_proxy.__file__`. It does not disturb other workers on the shared tree. T3 killed 6 of 6 mutants this way; the coordinator reproduced one.
- A timeout-forwarding test must assert elapsed time equals the configured value and use distinct values, because the default also times out. A test that still passes after the behaviour is removed cannot fail.
- `python -I` subprocess tests (in `tests/test_sansio.py`) ignore `PYTHONPATH` and load the repo source through the `.pth` file, so a scratch-copy mutation does not reach them. Mutate the live file only when no worker is running, with a backup and a sha256 restore check.
- Shared tree: concurrent workers can see each other's half-finished edits. Rerun once, report, do not fix out-of-scope files.

Settled decisions and known pre-existing issues are in the Decisions table of `task_plan.md` and under "Open items noticed, not acted on" above.

