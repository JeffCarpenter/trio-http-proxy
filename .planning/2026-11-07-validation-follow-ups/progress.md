# Progress

## 2026-11-07
- User approved all five follow-ups; created a separate plan without overwriting previous review edits.
- Locked Ruff 0.17.4 and Pyright 1.1.414. just lint-ci/lint use uv run --locked. just test invokes pytest once with source branch coverage.
- Coverage baseline and configured gate both pass: 212 tests; 279 runtime statements, 68 branches, 100%. Only static TYPE_CHECKING imports and an impossible TYPE_CHECKING branch are exempt.
- Added AST policy and snippet/CLI tests: 26 passed after observing the missing-module red phase. Existing test ASTs pass the gate. just ci includes no-monkeypatch.
- Moved the dated verifier into scripts/verify_mutations.py and added just mutation. Expanded from ten combined mutants to 16 granular mutants. Added 22 construction/result-classification tests (red before implementation).
- First mutation run exposed an overbroad lowercase timeout substring check: traceback identifiers idle_timeout triggered false rejection. Reproduced with actual JUnit attributes; added a failing regression and narrowed to timeout failure indicators. All 16 mutants now die by assertions (about 0.4s each); scratch control passes.
- CLI Pyright includes scripts and reports zero errors/warnings. Ruff found only formatting/import order in new files; corrected.
- Completed LSP investigation: all three prior .streams consumers and helper are published/clean before and after a scoped Python-only refresh, with indexing false on final reads. Definition paths match tests/streams.py and repository .venv. Host Pyright is 1.1.412 versus locked CLI 1.1.414; no cause proven or speculative fix made. Evidence/reproduction procedure is in lsp_investigation.md.
- Final local review found annotated-alias and callable setattr gaps. Observed three new failing regression cases, then fixed both patterns; AST suite now has 30 passing cases. Reviewer-agent tooling is not available in this session.
- Updated README.md and docs/validation.md with locked commands, strict coverage, AST limitations, separate mutation gate, and LSP diagnosis steps.
- Initial final gate stopped on Ruff formatting of a Python fence in lsp_investigation.md. Formatted that file; the complete rerun passes.
- Final verification: just ci passes all checks and 264 tests in 3.19s; 100% of 279 statements and 68 branches. just mutation passes an unmodified control and 16/16 assertion-only kills, about 0.4s each, no timeouts. git diff --check and git diff --exit-code -- src/trio_http_proxy both pass.
- Initial six plan phases complete. No commit created; earlier review-planning edits preserved.

## Crew review continuation
- User requested a crew. Spawned fina (AST), demo (mutations), and lobo (gates/docs) as read-only reviewers. User prefers concise Wenyan Chinese for crew communications; relayed to all.
- fina found constructor-parameter shadowing and typing.Any false positives plus missed simple callable aliases. Coordinator independently reproduced all three with harmless check_source snippets.
- lobo found no gate/docs issue; independently checked the strict coverage threshold and actual subprocess branch-data merging in isolated temporary files.
- demo found validation asserts disappear under optimization. Coordinator independently proved empty scratch origin checking fails closed normally, but PYTHONOPTIMIZE=1 permits tests against working source and returns 0.
- Assigned narrow, nonoverlapping fixes: fina owns only AST script/tests; demo owns only mutation script/tests. Both require red/green regressions; mutation tests must use explicit runner injection rather than monkeypatches. Coordinator handles integration, planning/docs, and final gates.
- fina completed scope-aware callable tracking, parameter shadowing, simple callable aliases, and typing-only/transparent-wrapper handling. Observed red 14 failed/31 passed, then wrapper red 3 failed/55 passed; final focused suite 58 passed.
- demo replaced all verifier acceptance/origin asserts with explicit RuntimeError checks, resolving actual paths. Added keyword-only runner injection and optimized subprocess regressions; observed red 10 failed/22 passed, final focused suite 32 passed. No monkeypatches used.
- lobo independently re-reviewed both fixes and six extra AST probes; no new actionable finding, 90 combined focused tests passed.
- Coordinator corrected two Ruff formatting layouts in the AST file, then ran the full chain: just ci passes 302 tests in 4.07s, all checks clean, strict source coverage remains 279 statements/68 branches at 100%. Normal mutation and PYTHONOPTIMIZE=1 mutation each pass control and kill all 16 by assertions, no timeouts. Diff whitespace/runtime-source checks pass.
- Updated docs/validation.md for optimization-safe checks and synchronized planning records. No runtime source changes, no commit, no user changes discarded.
- Retired only this crew (fina/demo/lobo); hcom lists them stopped. Other agents left untouched. Final documentation formatting and diff checks pass.

## Commit authorization
- User requested a commit once ready. Commit only the validation follow-up files (including the dated verifier promotion); preserve earlier review-planning document edits and CSV outside the index.
- Fresh pre-commit verification passes: just ci 302 tests in 3.85s, 100% source coverage; normal and PYTHONOPTIMIZE=1 mutation verification each pass control and kill 16/16. Formatting, diff whitespace, and no-runtime-source-diff checks pass. Commit preparation complete; resulting commit identity is recorded in git history.
