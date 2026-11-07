# Micro CONNECT proxy

## Goal
Create and check out micro from the best of nano/mini; reduce implementation complexity without weakening CONNECT or Trio lifecycle guarantees.

## Current phase
All phases complete.

### Phase inspect
**Status:** complete
Compare branches, run baseline checks, select nano (f87d9b9), define conservative scope.

### Phase implement
**Status:** complete
Unify forwarding into one bounded pipe; remove callback hooks and redundant timeout wrapper; update callers, exports, docs, and deterministic regressions. Keep parser, handshake and error wire behavior.

### Phase verify
**Status:** complete
Run formatting, lint, Pyright, all tests, coverage-guided lifecycle review, CLI smoke checks, repeated TCP tests, and final diff review.

## Next Step
No implementation work remains. User authorized committing the verified changes on micro; no push authorized.

## Decisions made
- Base micro on nano: mini is its ancestor and would reintroduce removed optional layers.
- Preserve h11, Python 3.14+, CONNECT validation, early bytes, all timeout bounds, half-close, sibling cancellation and cancellation-safe ownership.
- Remove redundant streaming APIs instead of protocol checks; no new runtime dependencies.
- User authorized the commit after verification. Do not push without separate authorization.

## Errors encountered
| Error | Attempt | Resolution |
| --- | --- | --- |
| Skill path under .agents missing | 1 | Read python-resilience from its listed .pi path. |
| Pyright rejected new test kwargs as potentially float bufmaxsz | 1 | Replace dict unpacking with explicit keyword arguments. |
| Ruff found test formatting after function rename | 1 | Format both affected test files. |
