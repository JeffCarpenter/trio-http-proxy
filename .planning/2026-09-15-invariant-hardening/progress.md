# Progress

## Completed

- Verified original baseline: `uv run pytest -q` produced 93 passing tests.
- Audited Trio serving, h11 parsing, cashews expiry, cache policy, and bridge-lifecycle behavior.
- Added `.worktrees/` to `.gitignore` in commit `d464577`.
- Accepted and integrated the proxy/server tranche:
  - source commits: `67f2160`, `e3365e5`
  - main commits: `a76c3e4`, `97c8c50`
  - independently verified on main: 114 tests passed, Pyright 0, scoped Ruff clean
  - covers exact custom-cache propagation and reset, pre-accept admission, overload retries, destination cleanup, byte-exact header limits/trailing data, deterministic invalid-target 400, and multi-address fallback within one deadline
- Native Luna diagnostics pass made no changes because native Ruff/Pyright were already clean; the earlier mcpls test error was a wrong-workspace result.
- Rejected the original RW2 cache draft after four pytest processes hung and the worker timed out without a commit. Saved its diff as `RW2-timeout.patch` and reset the worktree cleanly to main.

## In progress

- RW5 native Luna cache redo in `.worktrees/cache-core` / `invariant/cache-core`:
  - deterministic fail-open/fail-closed backend policy
  - actual-expiry rate-limit metadata
  - verified bridge task cancellation, loop closure, and thread termination
  - stop-versus-submit race coverage

## Next

1. Collect and independently audit RW5, including process/thread-level shutdown probes.
2. Integrate only if focused/full tests, Pyright, Ruff, scope, and lifecycle evidence pass.
3. Dispatch the dependent HTTP/config/docs tranche for 503 mapping and dynamic 429 `Retry-After`.
4. Run combined full pytest, Pyright, Ruff, mcpls diagnostics, and final scope/commit checks.
