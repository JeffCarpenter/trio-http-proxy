# Validation follow-ups implementation plan

## Goal
Complete all five approved validation follow-ups without changing proxy runtime behavior or overwriting earlier planning edits.

## Next step
Implementation and crew follow-ups complete; commit preparation verified. Create the authorized validation-only commit; leave earlier review-planning edits unstaged. The resulting commit is recorded in git history.

## Phases (stable step IDs)
1. `plan`: completed. Discovery/design; user approved the five-item approach.
2. `gates`: completed. Locked tools and single pytest source/branch coverage gate.
3. `policy`: completed. AST checker, snippet/CLI tests, and CI recipe.
4. `mutations`: completed. Permanent granular catalog and result-classification tests.
5. `lsp`: completed. Non-reproducing discrepancy documented with before/after refresh evidence and recurrence probe; no speculative fix.
6. `verify`: completed. README/validation documentation and local review complete. `just ci`: 264 passed, 100% of 279 statements/68 branches; `just mutation`: passing control and 16/16 assertion kills; diff checks pass.

## Crew follow-up phases
7. `crew-review`: completed. Three read-only reviews; three AST issues and one optimized-mode verifier issue reproduced. CI/coverage/docs review found no issue.
8. `crew-fixes`: completed. fina fixed AST scope/typing/aliases (58 tests); demo fixed optimized-mode acceptance checks with explicit runner injection (32 tests). Both demonstrated red/green regressions; lobo independently re-reviewed both, no further actionable issue.
9. `crew-verify`: completed. Combined `just ci`: 302 passed, 100% of 279 statements/68 branches. Normal and PYTHONOPTIMIZE=1 verifiers each pass control and kill 16/16 by assertions. Diff checks pass; records synchronized.

## Commit phase
10. `commit-validation`: verification complete. Fresh `just ci` passes 302 tests and 100% source coverage; normal/optimized mutation checks each kill 16/16; diff checks pass. Commit only tool/config/script/test/doc changes, this plan, and the verifier promotion; exclude earlier review-planning document edits and CSV.

## Design decisions
- Lock Ruff and Pyright as dev dependencies; use `uv run --locked` in required recipes.
- Keep one pytest invocation in `just ci`. Enforce 100% source line/branch coverage and preserve child-process coverage merging. Exempt only static imports and an impossible TYPE_CHECKING branch, never runtime paths.
- Use a pure AST checker with a thin CLI. Reject common patch APIs and dependency/method writes; allow fake state and explicit injection. Document limits rather than claiming a sandbox.
- Promote the dated verifier to `scripts/verify_mutations.py`; expose `just mutation` separately from CI. Split three forwarding sites and five timeout operands. Preserve scratch isolation, drift checks, import-origin checks, passing control, assertion-only kills, and short timeouts.
- Compare live mcpls against locked CLI Pyright empirically and refresh only Python. Record non-reproduction if there is no demonstrated configuration defect; no diagnostic suppressions.

## Verification and review
- Red/green tests for AST snippets/CLI and mutation construction/classification.
- Full `just ci`, then `just mutation`, plus `git diff --check` and no runtime source diff.
- Review alias/method handling and mutation failure classification. Final review found annotated aliases and callable setattr writes were missed; added three failing regressions before the fix (now 30 AST tests).
- Initial review was local because no native reviewer-agent tool was available. After the user enabled hcom and requested a crew, fina/demo/lobo performed independent reviews, scoped fixes, and re-review. All four confirmed findings are resolved.

## Existing work to preserve
Earlier review-planning document edits and validation_followups.csv belong to the pre-existing work. Keep them intact. The dated mutation verifier is promoted rather than duplicated; historical report commands refer to its original location.
