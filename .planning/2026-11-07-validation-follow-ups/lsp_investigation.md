# Live LSP / CLI Pyright investigation

## Result
The earlier unresolved `.streams` imports did not reproduce. No configuration change or diagnostic suppression was justified. The cause of the earlier discrepancy remains unconfirmed.

## Observations (2026-11-07)
- Locked CLI: Pyright 1.1.414, invoked with `uv run --locked pyright --verbose`, loaded this repository's `pyproject.toml`, Python 3.14, `.venv`, and `src`; reported zero errors and warnings.
- mcpls config at `~/.config/mcpls/mcpls.toml` launches `pyright-langserver --stdio` from PATH. The installed host Pyright version is 1.1.412. These are different versions, but there is no evidence that the version difference caused the prior diagnostic.
- The Python language-server process runs with this repository as its working directory. A separate Python server for another project was left alone.
- Before refresh, mcpls returned published, empty diagnostics for `tests/test_proxy.py`, `tests/test_tunnel_cleanup.py`, `tests/test_tunnel_forwarding.py`, `tests/streams.py`, and both new verification scripts. `indexing_in_progress` and `push_notifications_degraded` were false.
- Go-to-definition on `RecordingStream` in `tests/test_tunnel_forwarding.py:15` resolved to this repository's `tests/streams.py:8`.
- Go-to-definition on the `trio` import in `src/trio_http_proxy/proxy.py:10` resolved to this repository's `.venv/lib/python3.14/site-packages/trio/__init__.py`. This confirms dependency resolution agrees with the configured environment; the bridge does not expose a full effective interpreter/settings dump.
- Refreshed only server `python` with mcpls `restart_server`. Initial reads were published/empty but still indexing, so they were not treated as final evidence. Later reads for all three `.streams` consumers and the helper were published/empty, not indexing, and not degraded. Import definitions still resolved correctly.

## Minimal reproduction procedure if the discrepancy recurs
The existing two-file import is the smallest useful probe here:

```python
# tests/streams.py
class RecordingStream: ...


# tests/test_tunnel_forwarding.py
from .streams import RecordingStream
```

Keep the real files and current configuration in place; do not rewrite runtime code to match this sketch.

1. Run `uv sync --locked`, then `uv run --locked pyright --verbose`. Save the loaded configuration, version, search paths, and diagnostics.
2. Query live mcpls diagnostics for `tests/test_tunnel_forwarding.py`, plus go-to-definition on `RecordingStream` and a `trio` import. Save the diagnostic code/range and resolved paths.
3. Inspect the Python server's executable version and workspace working directory. Compare against `.venv`, not an unrelated host interpreter.
4. Refresh only the Python server, then repeat after indexing completes. An empty result with pending/evicted availability, active indexing, or degraded push notifications is inconclusive.
5. If the mismatch persists, file a tooling reproduction with those outputs and the import/configuration files. Do not add ignores or change valid imports without demonstrating a project error.

This is a non-reproducing investigation record, not a claim that a refresh fixed the original issue.
