# Findings

## Scope and baseline
- Reviewed existing production code through reuse, quality, efficiency, and altitude lenses, not just the latest commit: the user explicitly requested codebase-wide work.
- Initial tree clean; HEAD 44d949d. Baseline: 301 pytest tests passed in 6.72s; Ruff passed; Pyright reported zero errors.
- Four review angles performed inline because no delegation tool was available. Inspected history before removals, especially recent dependency-injection work.

## Original cleanup risk classification
The original cleanup applied CAREFUL duplicate consolidation or local control-flow simplification, protected by focused contract tests and the full suite. Lifecycle changes were initially deferred; the user-authorized follow-up below undertook that structural work.

## Original cleanup verification
344 tests passed in 8.23s; Ruff, Pyright, git diff --check, and published LSP diagnostics for changed production files passed. Net production reduction: 27 lines across four files. New contract coverage: 43 cases. No benchmark claim.

## Applied findings
| Lens | Evidence | Decision and validation |
|---|---|---|
| Reuse / efficiency | proxy.py rebuilt the same HTTP 429 wire template already exported by http.py. | Reuse the bytes template and consolidate Retry-After conversion; preserve numeric clamping, NaN fallback, and exception metadata. Ten wire-format cases. No throughput claim. |
| Quality / reuse | tunnel repeated result recording, disconnect-safe response sending, and exception propagation for 502 and 504; retry branches had identical bodies. | Share the common tail and combine retry catches. Existing timeout/connect/logging tests plus two explicit tests verify both attempted addresses, one recorded failure, original exception identity, permit release, and exact response bytes. |
| Reuse / quality | CircuitPermit.__exit__ duplicated abort's state checks and release side effects. | Delegate to abort; six cases cover normal/exceptional exit, prior completion, prior abort, and repeated exit/abort calls, alongside formal breaker invariants. |
| Reuse | Three positive duration fields used the same validation, but rate_limit_period had its own implementation. | Add the field to the existing validator; nine invalid-value cases verify field-specific error locations and messages. Existing settings/builder tests cover valid values. |
| Quality | ConnectionResultBuilder.record_to and proxy._record_connection_result each repeated awaitable handling in modern/legacy branches. | Hoist each identical awaitable tail without merging distinct dispatch rules. Sixteen cases cover record/builder, modern/legacy, sync/async, success/failure. Preserve all compatibility paths. |

## Initially deferred or rejected
- Do not replace the cache bridge's lifecycle flags with a single state enum during cleanup: history and tests demonstrate coordination across start, stop, degradation, and worker threads; this is a separate behavioral refactor.
- Retain compatibility re-exports, default-cache/context sentinels, deprecated builders, and dynamic recorder dispatch. They support intentional public and injected contracts, not dead code.
- Do not merge builder and direct-record dispatch into a universal adapter: their unsupported-object behavior differs. Hoisting awaitable handling locally avoids silently changing that contract.
- Do not remove DNS resolver/thread seams or closures merely to reduce lines: they isolate injected dependencies and asynchronous work.
- Avoid a new generic logging or response abstraction: the chosen local common tails remove repetition without raising abstraction cost.
- No speculative concurrency, timeout, caching-policy, or public API changes. No measured performance improvements claimed.
- Formatter check reveals existing drift (including new Ruff Python 3.14 exception syntax). Format the new test file and added multiline expressions, but do not mix whole-file style churn into semantic cleanup.

## Follow-up structural refactor (user-authorized)
- LifecycleState already exists, but _started and _stopped duplicate its status; _stopping represents a different concept: one caller owning shutdown. Replace duplicate flags with enum checks and represent ownership using a lock, not another lifecycle state.
- STOPPING must reject new work yet remain retryable when join times out. STOPPED must mean resources have actually been released.
- Backend degradation remains orthogonal to lifecycle: fail-open RUNNING may be degraded with a live bridge.
- Recorder paths intentionally differ for malformed legacy implementations: direct records skip a non-callable record_result, while deprecated builders raise. Centralize actual method selection/invocation with an explicit compatibility policy rather than silently homogenizing the behavior.
- Keep injectable backend/resolver/clock/thread factories, public exports, context precedence, and builder warnings.
- Basic threat model: shutdown races must not admit new work, leak worker threads, resurrect a stopped cache, or report complete shutdown while resources remain live.

## Follow-up implementation and evidence
- Replaced _started, _stopped, _stopping, and _stop_complete with authoritative enum admission and explicit shutdown ownership. Health remains independent.
- Preserved one cleanup future so a timed-out or failing join cannot trigger a second shutdown coroutine that cancels an in-flight backend close.
- Shared submission checks and scheduling under the state lock; stopped factories are not invoked and rejected pre-created coroutines are closed.
- Startup uses cancellation-aware submission for ping and drains a loop published at the startup deadline. Concurrent stop cannot be overwritten by readiness publication.
- Both recorder entry points now use one dispatcher with explicit strict_legacy policy; precedence, non-callable behavior, awaitable handling, disabled recording, warnings, and exports retain their contracts.
- Seven lifecycle cases plus 52 simplification/recorder cases supplement the baseline 301 tests, totaling 360. All passed normally and with warnings as errors; lifecycle cases also passed 30 repetitions (210 executions).
- Stricter verification exposed cancellation interrupting an existing invariant test's socket cleanup. Shielding its try/finally teardown fixed four leaked test clients without modifying server semantics.
- Final Ruff, Pyright, diff whitespace checks, and six published LSP diagnostic reads passed. Four changed production files have a combined net reduction of 76 lines against HEAD; no throughput claim.
- docs/cache-lifecycle.md records transitions, startup-abort behavior, health separation, cleanup ownership/retry, and recorder compatibility; docs/testing.md links to it.
- Remaining operational limit is deliberate: cancellation is cooperative. Backend cleanup or executor work that will not finish cannot be forcibly terminated safely; bounded stop attempts report failure and remain retryable in STOPPING rather than falsely claiming STOPPED. Startup abort before bridge acquisition is terminal, with a delayed worker responsible for observing the abort event and exiting.

## Test-development correction
The first recorder matrix fixture supplied both an error and a permit, which ConnectionRecord intentionally rejects. Corrected the fixture to test separate success-with-permit and failure-with-error records; all 43 added cases passed before the recorder-tail changes.
