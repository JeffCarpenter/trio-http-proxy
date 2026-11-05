# Progress

## Audit
- Read testing guidance, existing TCP/TLS stress fixtures, backend effect adapters, and bridge lifecycle implementation.
- Ran baseline `just ci`: Ruff/Pyright clean; 373 tests passed.
- Created this plan and selected it as the active plan.

## Implementation
- Added four real-TCP tests: the permanent smoke scenario and three churn/shutdown cycles (252 stress connections total).
- Added eight bridge stress cases: three repeated cancellation cases (144 pending reads), three concurrent-close cases (48 pending reads), and two concurrent-outage policies (32 reads).
- Assert handler/socket/listener cleanup, returned capacity tokens, bridge/loop/executor termination, backend-close ownership, and bridge task baseline restoration.
- Documented focused test and branch-coverage commands in docs/testing.md.
- Initial task-count assertions incorrectly expected an empty running loop. Real Cashews has an expiration task; tests now compare against startup task identities and verify their completion at shutdown. No production changes needed.

## Verification
- New integration/stress tests: 12 passed.
- Ruff and formatter checks on new files pass.
- `just ci`: Ruff clean, Pyright 0 errors/warnings, 385 tests passed in 6.66s.
- Broader lifecycle/integration/TLS stress subset: 84 tests passed on each of five consecutive runs (420 successful executions, 4.97–5.35s per run).
- Full branch-coverage suite: 385 passed in 7.97s. Branches 385/492 (78.25%); statements 1641/1826 (89.87%); combined 87.40%. Detailed gaps and report paths recorded in findings.md.
- New tests with `PYTHONASYNCIODEBUG=1` and `-W error`: 12 passed in 0.51s; no warnings.
- Published LSP diagnostics empty for both new test modules, without indexing/degradation.
- Final Ruff/format checks and `git diff --check` clean. Reviewed the new test modules and documentation; corrected shell continuation escaping in the documented coverage command.
- No production edits, new dependencies, application patches, or mocking-library imports.

## Final commit review
- User authorized a commit after a final confidence check; pushing was not requested.
- Reviewed both new test modules, documentation, and planning records; no blocking findings.
- Repeated `just ci`: Ruff clean, Pyright 0 errors/warnings, 385 passed in 6.77s.
- Repeated new tests with asyncio debugging and warnings as errors: 12 passed in 0.62s.
- Both new modules remain Ruff-format clean; `git diff --check` passes.
- Prepared the tests, documentation, and completed planning records for the authorized commit.
