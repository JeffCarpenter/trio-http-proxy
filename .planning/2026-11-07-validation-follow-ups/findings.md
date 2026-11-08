# Findings

- Existing dev dependencies include coverage>=7.16.2 and pytest-cov but not Ruff/Pyright. just lint-ci/lint use host Ruff; typecheck already uses uv run --locked.
- tests/test_cli.py explicitly merges child coverage into Coverage.current(), including branch arcs. Preserve this real module-entry mechanism.
- Existing verifier is a dated planning script: ten mutations, combined timeout guard removal and three combined bufmaxsz removals. It already validates import origin, passing control, pytest return code, JUnit assertions and no timeouts.
- tests/test_sansio.py parameterizes all five invalid timeout operands. test_http_proxy_forwards_bufmaxsz observes handshake and both pipe directions.
- Original review planning files and validation_followups.csv have pre-existing uncommitted edits; do not overwrite them.

## Final findings
- Locked tools: Ruff 0.17.4 and Pyright 1.1.414. Required Python quality commands no longer depend on host-installed Ruff/Pyright.
- Coverage configuration enforces 100% source lines/branches with only static TYPE_CHECKING exemptions. Existing CLI tests continue to collect real subprocess coverage.
- AST guard: 30 harmless snippet/CLI tests, including ordinary fake state/injection, imported/simple/annotated aliases, patch fixtures, and callable setattr replacement. Dynamic reflection and complex alias flow remain manual-review limitations, documented in docs/validation.md.
- Permanent mutation verifier: 16 individually compiled mutations, five timeout operands and three forwarding sites independent. 22 regression tests exercise construction, drift, controls, and assertion-only classification. Actual run kills all 16 by assertions with no timeout or collection-error kills.
- Live LSP discrepancy does not reproduce before or after a Python-only refresh. Published/empty diagnostics after indexing; helper resolves to tests/streams.py, trio to this repository's .venv. Host Pyright is 1.1.412 versus locked CLI 1.1.414; causation is unproven. See lsp_investigation.md for evidence and recurrence probe.
- Final just ci: 264 passed in 3.19s; 279 statements and 68 branches, zero missing/partial, 100%. Ruff/Pyright/Sans-io/AST checks pass. just mutation: passing scratch control and 16/16 assertion kills, each about 0.4s. git diff --check and source-only diff check pass.
- No proxy runtime source changes or diagnostic suppressions. Original review-planning edits remain intact; its verifier was promoted to scripts rather than duplicated.

## Crew review findings (validated)
- AST callable collection is file-global: a constructor parameter shadowing a same-named function falsely triggers the method-replacement rule.
- Annotations such as typing.Any are wrongly treated as imported runtime origins, so ordinary fake-state writes can be rejected.
- Simple callable aliases bypass method replacement because the rule tests the raw value name rather than its resolved callable binding.
- Mutation verifier safety uses Python assert, including its child import-origin bootstrap; PYTHONOPTIMIZE removes these checks. Empty scratch reproduced a real origin-check bypass, not merely synthetic XML classification. Fix all acceptance checks explicitly and test optimized execution.
- CI/coverage/docs review independently confirmed strict 100% enforcement and real child branch merging; no scoped issue.

## Crew resolution and final verification
- All four findings resolved: scope-aware callable/alias state with parameter shadowing; typing-only annotations exempt while transparent wrappers retain concrete dependency protection; explicit optimization-safe verifier failures and narrow runner injection.
- AST has 58 regression cases; verifier has 32. Both owners observed failing tests before fixes. lobo independently reviewed changes and extra boundary probes; no new actionable finding.
- Final coordinator run: just ci passes 302 tests in 4.07s; Ruff, Pyright, Sans-io and AST gates pass; 279 source statements and 68 branches at 100%. Normal and PYTHONOPTIMIZE=1 mutation runs each pass control and kill 16/16 by assertions with no timeouts. Diff checks pass; proxy runtime source unchanged.
- Optimization nuance: -O on a parent interpreter does not propagate to run_tests children; PYTHONOPTIMIZE=1 does. Regression tests cover both parent optimization and genuinely optimized child origin validation.
