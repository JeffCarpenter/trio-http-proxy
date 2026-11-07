# Progress

## Actions taken
- Compared nano and mini; selected nano f87d9b9 and checked out micro.
- Replaced callback/wrapper forwarding with one bounded pipe; runtime source is 400 lines (was 452), streaming.py is 38 (was 89).
- Migrated callers and exports; retained protocol/handshake/lifecycle behavior. Documented breaking streaming API and reduced logging.
- Updated current branch docs and package/CLI descriptions, marked historical mini spec explicitly.
- Added virtual-clock regressions for default/custom receive/send/EOF deadlines, outer cancellation in each phase, independent renewed budgets, ownership, and no residual deadlines. Added propagation checks for unexpected EOF errors and a sibling-cancellation/cleanup regression for stalled half-close.

## Files created or modified
- src/trio_http_proxy/{streaming.py,proxy.py,__init__.py}
- tests/{test_streaming.py,test_invariants.py,test_cli.py,test_proxy_tcp_integration.py}
- README.md, README.rst, docs/configuration.md, AGENTS.md, CLAUDE.md, meta.md, pyproject.toml
- Planning files under .planning/2026-11-07-micro-connect-proxy/.

## Test results
- nano baseline: just ci passed; Ruff/Pyright clean, 92 tests.
- Refactored micro before new tests: 92 tests passed; coverage 95% overall, streaming 100% statement/branch.
- Initial 11 virtual-clock cases passed; initial full static run caught overly broad kwargs inference in new tests; replaced with explicit keywords.
- Final just ci: Ruff/Pyright clean, 106 tests passed (14 cases added). Formatting and diff whitespace checks passed. Repeated successfully immediately before the authorized commit.
- Final coverage: streaming 100% statement/branch; overall runtime 95%.
- TCP integration suite: 10 consecutive runs, all 50 cases passed.
- Console-script and module --help smoke tests passed. Git verifies unchanged parser, wire constants, exception hierarchy, and lockfile; final diff reviewed.

## Error log
- Missing skill path corrected (.agents -> .pi for python-resilience).
- New test kwargs inferred too broadly by Pyright; explicit keywords fix intent without type ignores.
- Formatted tests after API rename shortened a call.
- uv emits a pre-existing build-system/current-version warning; locked install and build succeed.

## Reboot-check summary
On micro from nano f87d9b9 (mini ancestor c812a7b). All work and verification complete; user authorized committing the verified changes. See git log for the resulting commit. No push authorized. Runtime source is 400 lines with 106 passing tests.
