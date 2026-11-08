# Task Plan: Review Rectification

## Goal
Rectify all code-review findings across `src/trio_http_proxy/` while strictly preserving structured concurrency invariants, the sans-I/O boundary, and existing test contracts, backed by targeted regression tests.

## Next Step
Done. Pushed `nano` to `origin/nano` through `5c959fe`; ROI-ranked backlog written to `.planning/2026-11-07-review-rectification/next_steps.yaml`.

## Current Phase
Phase 5 (complete)

## Phases

### Phase 1: Requirements & Discovery
- [x] Perform rigorous, probing code review of all `src/trio_http_proxy/` modules using `mcpls` and repro probes
- [x] Verify existing test suite contracts across `tests/` so no intentional `nano` behavior is broken
- [x] Document findings and root causes in `findings.md`
- **Status:** complete

### Phase 2: Planning & Structure
- [x] Define exact changes for `protocol.py`, `timeouts.py`, `streaming.py`, `proxy.py`, and `docs/configuration.md`
- [x] Design regression tests for every rectified defect
- **Status:** complete

### Phase 3: Implementation
- [x] Update `src/trio_http_proxy/protocol.py` (`parse_connect_target` RFC/IDNA/control-octet hardening, `ConnectParser` `maxreqsz >= 1` guard, reject `Content-Length`/`Transfer-Encoding` on `CONNECT`)
- [x] Update `src/trio_http_proxy/timeouts.py` (`PipeTimeouts` non-negative finite timeout validation)
- [x] Update `src/trio_http_proxy/streaming.py` (`pipe()` EOF check before `on_receive()`, parenthesized exception tuple)
- [x] Update `src/trio_http_proxy/proxy.py` (`_reply_error` best-effort write error suppression, `UnicodeError` defense-in-depth, `bufmaxsz` forwarding in `tunnel`/`http_proxy`, `main()` `--port` range check)
- [x] Update `docs/configuration.md` (`bufmaxsz` parameter on `tunnel` and `http_proxy`)
- **Status:** complete

### Phase 4: Testing & Verification
- [x] Add regression tests across `tests/test_protocol.py`, `tests/test_sansio.py`, `tests/test_tunnel_cleanup.py`, `tests/test_tunnel_forwarding.py`, `tests/test_pipe_deadlines.py`, `tests/test_cli.py`, and `tests/test_proxy_tcp_integration.py`
- [x] Run `mcpls` `get_diagnostics` on all modified files
- [x] Run `just ci` (`ruff format --check`, `ruff check`, `just sansio`, `pyright`, `pytest`)
- **Status:** complete

### Phase 5: Delivery
- [x] Summarize rectifications and verification results for the user
- **Status:** complete

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| Reject ASCII control chars/whitespace (`<= 0x20`, `>= 0x7f`), `%`, `\`, and IDNA-invalid hostnames in `parse_connect_target` | Prevents `urlsplit` from silently stripping `\r`/`\n`/`\t`, rejects embedded NUL/spaces/IPv6 zone IDs, and prevents `socket.getaddrinfo` from raising uncaught `UnicodeEncodeError` that crashes `trio.serve_tcp` |
| Catch `UnicodeError` alongside `OSError` in `tunnel()` | Defense-in-depth so even if a custom or future path reaches `open_tcp_stream` with an unencodable host, it replies `502` and is caught by `http_proxy` instead of crashing the server nursery |
| Reject `Content-Length` and `Transfer-Encoding` headers on `CONNECT` in `ConnectParser._poll()` | RFC 9110 §9.3.6 forbids content on `CONNECT`; prevents HTTP request smuggling / framing desynchronization |
| Suppress `(trio.BrokenResourceError, trio.ClosedResourceError, OSError)` in `_reply_error` | Honors `tunnel()`'s "send an HTTP error when possible" contract without masking the primary `ProxyProtocolError`, `TooSlowError`, or upstream `OSError` |
| Move `if not chunk: break` before `on_receive()` in `pipe()` | Prevents EOF (`b""`) from transitioning `PipeTimeouts` into the `"send"` phase when no chunk will be sent |
| Preserve `RequestTooLargeError -> 414`, `receive_data(b"")` no-op, single handshake `send_timeout`, and per-direction `idle_timeout` | Explicitly pinned by existing tests (`test_sansio.py`, `test_tunnel_forwarding.py`, `test_invariants.py`) and `docs/configuration.md` |

## Errors Encountered
| Error | Resolution |
|-------|------------|
| `ImportError: cannot import name 'StapledStream' from 'trio.testing'` during initial repro | Used `trio.StapledStream` |
| `mcpls` `get_diagnostics` reported `bytes \| bytearray` return type mismatch on `recording_receive_some` in `tests/test_tunnel_forwarding.py` | Annotated return type as `bytes \| bytearray` and used `monkeypatch.setattr` |
| `ruff check` `PLR0124` (`now == now`) in `timeouts.py` | Replaced with `math.isnan(...)` |
