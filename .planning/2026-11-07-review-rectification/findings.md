# Findings & Decisions

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
     - Raise `ValueError` in `PipeTimeouts.__init__` when `not (idle_timeout >= 0)` or `not (send_timeout >= 0)` or `not (now == now)`.

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
