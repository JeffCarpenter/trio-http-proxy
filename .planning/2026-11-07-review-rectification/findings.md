# Findings & Decisions

## Approved DI implementation — complete
- CLI now accepts an explicit Sequence[str] argv and typed runner, defaulting to normal process args and trio.run. No dependency container or settings object. Tests pass recording/interrupting runners and use subprocess env mappings rather than touching globals.
- Entry-point tests measure real child execution explicitly and merge CoverageData into the active collector through public APIs; no subprocess patch configuration. runpy import-only namespace must be package-qualified or coverage's source-package filter skips it. Initial 99% result resolved without branch exclusions.
- Shared RecordingStream accepts only a HalfCloseableStream, records read limits/successful writes/close calls/cancellation, and injects send behavior through one constructor callback. Normal I/O and EOF delegate unchanged. Cleanup assertions precede raw-resource teardown; success leaves upstream ownership with the caller. No production stream, cleanup or clock seams were needed.
- Replaced patched serve_tcp delegation with stronger existing live tests: default-host forwarding plus custom-host port-0 startup, readiness, CONNECT and cancellation. Both tunnel peers and listener closure are observed before fixture/client teardown; all six live TCP tests passed ten consecutive runs without sleeps.
- Source/AST audit across 13 test/support modules found no patching or dependency-method replacement. Remaining non-self attribute assignments only set fake failure/stall state. Sans-I/O import blocking remains a supported custom import finder installed only in a fresh child process, not replacement of a dependency function.
- Final gate: just ci passes 212 tests; CLI Pyright/Ruff clean; 100% source line/branch coverage (279 statements, 68 branches); all ten mutants die by assertions in 0.38–0.44 s.
- Live mcpls discrepancy: diagnostics refreshed for 12 affected Python files; nine clean, three unresolved .streams imports in consumers of the new helper. Fresh CLI Pyright includes src and tests and reports zero errors; runtime imports succeed. No suppressions or path workarounds added; live-server import resolution remains a tooling limitation.

## Follow-up backlog (2026-11-07, before DI migration)
- Pre-migration gate: 209 tests pass; 100% coverage across all eight source modules (279 statements, 68 branches). just ci, diff whitespace check, and published mcpls diagnostics on all six modified/new Python files are clean. No runtime defaults/ownership behavior changed.
- New properties explicitly exercise IDNA-invalid dot/label targets and valid IPv6/domain hosts. The shared-nursery property combines arbitrary bytes, structured requests, varied chunking and six injected dial outcomes (success, OSError, UnicodeError, BrokenResourceError, ClosedResourceError, timeout); a sibling must remain alive. MockClock bounds runtime without network or patches.
- The initial DI proposal was documented in testability_plan.md and next_steps.yaml rank 5. The user subsequently approved it; implementation above clears the identified patch-based CLI/cleanup debt.
- Mutation verification: all ten grouped backlog mutants killed by explicit assertions, not collection/runtime errors or timeouts. Added a reusable isolated verifier and mutation_report.md; clean-copy control passes. Error-reply test uses an injected faulty SendStream; the later approved argv/runner seam also removes CLI patching.
- User's testability direction: propose small DI seams to remove monkeypatching over time; no dependency container or restored settings layer. Prioritize CLI argv/runner injection, recording/fault-injecting stream wrappers, observing upstream aclose through wrappers, and reuse existing _connect. Prefer a real loopback test to injecting serve_tcp solely for delegation.
- Configuration hygiene: pyproject.toml now owns Pyright settings, includes src/ and tests/, and retains explicit Python 3.14 and project venv resolution without hard-coded site-packages. Deleted shadowing JSON. just lint-ci checks formatting before lint; just ci passes (205 tests, zero type errors).
- mcpls call hierarchy confirms http_proxy -> tunnel -> process_as_connect_request, with _reply_error handling failures. Existing forwarding/cleanup tests exercise these boundaries.
- Baseline: 202 tests passed; 97% reported branch+line coverage.
- Real gaps: in-process package `__dir__` and both module-entry modes. Added three focused cases; subprocess entry/lazy-loading tests remain intact.
- Coverage.py 7.16.1 falsely reports two nursery/loop arcs on Python 3.14. Official changelog documents the fix in 7.16.2 (issue 2168). An isolated `uv run --locked --with 'coverage>=7.16.2' pytest ...` cleared the proxy misses before source edits. Require coverage>=7.16.2; lock resolves 7.17.2.
- The runtime-only `if not TYPE_CHECKING` guard cannot take its false branch. Marked only that branch `pragma: no branch`, retaining line and lazy-export body coverage; no runtime paths excluded.
- Coverage audit result: 205 tests pass, all eight source modules at 100% (279 statements, 68 branches; no missed statements/partial branches).


## Requirements
- Rigorously refactor `src/trio_http_proxy/` to rectify code review concerns.
- Honour the structured-concurrency gospel (every task in a nursery, clean cancellation propagation, deterministic stream ownership and teardown, no unhandled connection-local exceptions escaping into `trio.serve_tcp`'s nursery).
- Preserve the sans-I/O boundary (`protocol.py`, `timeouts.py`, `errors.py`, `http.py` must not contain the word `trio` or import `trio`).

## Code Review Defects & Rectifications

1. **Remote Server Nursery Crash on IDNA-Invalid Hostnames (`protocol.py`, `proxy.py`)**
   - `parse_connect_target(b".example.com:443")`, `b"..:443"`, `b"a..b.com:443"`, and 64+ char labels passed `urlsplit` and `desthost.strip()`, then caused `trio.open_tcp_stream` -> `socket.getaddrinfo` to raise `UnicodeEncodeError` (a `UnicodeError` -> `ValueError`), escaping both `tunnel()` and `http_proxy()` and crashing `trio.serve_tcp`.
   - Fix:
     - In `parse_connect_target`, validate non-bracketed `desthost` via `desthost.encode("idna")` (converting `UnicodeError` into `ProxyProtocolError`), and also reject empty labels / trailing-only dots.
     - In `tunnel()`, catch `(OSError, UnicodeError)` on `(_connect or trio.open_tcp_stream)(host, port)` and in `http_proxy()` catch `UnicodeError` as defense-in-depth.

2. **`parse_connect_target` Control Character, Whitespace, Zone-ID, and Dead-Code Cleanup (`protocol.py`)**
   - `urlsplit` silently strips `\r`, `\n`, `\t` (`b"example.com\r:443"` -> `("example.com", 443)`) and allows spaces/control chars (`\x00`..`\x1f`, `\x7f`) and `%` (IPv6 zone IDs).
   - Lines 47–55 checking `parsed.path`, `parsed.query`, `parsed.fragment`, `parsed.username`, `parsed.password` were unreachable dead code after the delimiter check.
   - Fix:
     - Reject any character with `ord(c) <= 0x20 or ord(c) >= 0x7F` before calling `urlsplit`.
     - Reject `"@"`, `"?"`, `"#"`, `"/"`, `"\\"`, `"%"` before calling `urlsplit`.
     - Remove the unreachable post-`urlsplit` check.
     - Reject bracketed hosts that are not valid IPv6 literals, and validate port digits (`port_str.isascii() and port_str.isdigit()`, no `+`/`-`/whitespace).

3. **HTTP Request Smuggling / Framing Headers on `CONNECT` (`protocol.py`)**
   - `ConnectParser._poll()` accepted `CONNECT` requests carrying `Content-Length` or `Transfer-Encoding` headers and forwarded raw body/chunk-framed bytes as `trailing_data`.
   - Fix:
     - Reject `CONNECT` requests with `content-length` or `transfer-encoding` in `event.headers` by raising `ProxyProtocolError`.

4. **`ConnectParser` and `PipeTimeouts` Parameter Validation (`protocol.py`, `timeouts.py`)**
   - `ConnectParser(maxreqsz=0)` succeeded in `__init__` and then failed on the first `next_request()` call with `RequestTooLargeError` before reading any bytes.
   - `PipeTimeouts` accepted negative or `NaN` `idle_timeout` / `send_timeout`.
   - Fix:
     - Raise `ValueError` in `ConnectParser.__init__` when `maxreqsz < 1`.
     - Raise `ValueError` in `PipeTimeouts.__init__` when `idle_timeout < 0`, `send_timeout < 0`, or any parameter (`idle_timeout`, `send_timeout`, `now`) satisfies `math.isnan(...)`.

5. **`pipe()` Calling `on_receive()` on EOF (`streaming.py`)**
   - `pipe()` called `on_receive()` before `if not chunk: break`, causing EOF (`b""`) to switch `PipeTimeouts.phase` to `"send"` even when no data was received or sent.
   - Fix:
     - Check `if not chunk: break` immediately after `chunk = await from_stream.receive_some(bufmaxsz)`, before calling `on_receive()`.
     - Parenthesize `except (trio.BrokenResourceError, trio.ClosedResourceError):` for consistency.

6. **`_reply_error` Exception Masking in `tunnel()` (`proxy.py`)**
   - When the client stream was already broken/closed, `await stream.send_all(response)` inside `_reply_error` raised `BrokenResourceError` / `ClosedResourceError` / `OSError`, masking the primary `ProxyProtocolError`, `trio.TooSlowError`, or upstream `OSError` in `tunnel()`.
   - Fix:
     - Suppress `(trio.BrokenResourceError, trio.ClosedResourceError, OSError)` inside `_reply_error` while allowing `trio.Cancelled` (`BaseException`) to propagate immediately.

7. **`bufmaxsz` Forwarding and CLI `--port` Validation (`proxy.py`, `docs/configuration.md`)**
   - Add `bufmaxsz: int = 8192` keyword parameter to `tunnel()` and `http_proxy()` and forward it to `process_as_connect_request` and `pipe_with_timeout`.
   - Validate `0 <= args.port <= 65535` in `main()` via `parser.error(...)`.

## Surprises & Retrospective Insights

1. **How `CONNECT .example.com:443` bypassed both `h11` and `urlsplit` to crash the entire Trio server nursery**:
   - In `trio.serve_tcp`, every client handler runs as a child task in the server's nursery, so any uncaught exception in `http_proxy` cancels all sibling client tunnels and terminates `start_server`.
   - `urllib.parse.urlsplit("//.example.com:443")` returned `hostname=".example.com"`, and CPython's `socket.getaddrinfo()` (inside `trio.open_tcp_stream`) ran `.encode("idna")` in C before calling the OS resolver. Because `UnicodeEncodeError` inherits from `ValueError` rather than `OSError`, `except OSError:` in `tunnel()` and `http_proxy()` missed it completely—even after 240,000 fuzz cases in `test_fuzz_protocol.py` (which generated valid `[a-z0-9]` labels and mocked `_connect`).

2. **`test_fuzz_protocol.py` already knew `Content-Length` and `Transfer-Encoding` were illegal on `CONNECT`, even though `ConnectParser` did not**:
   - `tests/test_fuzz_protocol.py:250-252` explicitly filtered `("host", "content-length", "transfer-encoding")` out of `_header_name_strategy`, yet `ConnectParser._poll()` had no check rejecting them, allowing `h11` to transition `their_state` to `SEND_BODY` while returning raw body/chunked framing bytes via `self._conn.trailing_data`.

3. **Silent input normalization in `urllib.parse.urlsplit` (bpo-43882) and resulting dead code**:
   - `urlsplit` silently strips `\r`, `\n`, and `\t` to follow the WHATWG URL spec, causing `parse_connect_target(b"exam\nple.com\r:443")` to succeed with `("example.com", 443)`.
   - Conversely, once `@`, `?`, `#`, and `/` were rejected before `urlsplit(f"//{decoded_target}")`, the subsequent check on `parsed.path`, `parsed.query`, `parsed.fragment`, `parsed.username`, and `parsed.password` could never be reached.

4. **`h11`'s `max_incomplete_event_size` interaction with `trailing_data` at the exact header limit**:
   - Passing `max_incomplete_event_size=maxreqsz` to `h11.Connection` does not falsely reject a small header block that arrives in the same chunk as a large TLS `ClientHello`, because `h11` checks `max_incomplete_event_size` only when `_extract_next_receive_event()` returns `NEED_DATA`. Once `\r\n\r\n` is present in the buffer, `h11` extracts `h11.Request` first and leaves `ConnectParser._poll()` (`self._received - len(trailing_data) > self._maxreqsz`) to measure strictly the header bytes.
