# Nano CONNECT proxy

## Goal
Reduce branch `nano` (based on `mini`) to CONNECT tunneling while preserving protocol validation and safe stream cleanup.

## Next Step
Complete. Review the diff and commit when requested.

## Steps
- [x] inspect: Inventory mini and baseline tests (217 passed).
- [x] implement: Remove ProxyConfig, destination policies, limiter, DNS racing and signal draining. Retain bounded parsing, timeouts, half-closes and cleanup.
- [x] verify: Adapt tests/docs; run Ruff, Pyright and pytest; review diff.

## Decisions
- Default listen address is 127.0.0.1:8080; CLI supports only host and port, no environment settings.
- Use trio.open_tcp_stream for DNS and Happy Eyeballs; trio.serve_tcp for lifecycle.
- Keep simple function parameters for testable safety limits; no settings object or compatibility layer.
- Remove tests for deleted features; retain property, invariant and live TCP tests.
