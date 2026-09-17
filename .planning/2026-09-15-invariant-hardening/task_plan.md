# Task Plan: Proxy Invariant Hardening

## Status

Implementation is active in two isolated RW worktrees. Baseline audit and semantics are documented in `findings.md`.

## Phases

- [x] Audit all ten invariants against code and tests.
- [x] Define boundary, timeout, cache-policy, retry, and shutdown semantics.
- [x] Create isolated branches/worktrees and dispatch disjoint RW tranches.
- [ ] Collect and independently validate proxy/server tranche.
- [ ] Collect and independently validate cache/lifecycle tranche.
- [ ] Integrate disjoint commits into main.
- [ ] Run dependent HTTP/cache integration tranche.
- [ ] Run final combined pytest, pyright, ruff, and mcpls diagnostics.
- [ ] Remove worktrees after acceptance.
