# Task Plan: Resume Root Proxy Enhancements

## Goal
Complete the root plan: accept graceful signal shutdown and incremental Hypothesis fuzzing, integrate target allowlist / SSRF protection, and run combined verification. Preserve existing root planning documents and all worktree changes. Coordinate bounded, disjoint workers where available.

## Current phase
Complete: all phases accepted and implementation pushed to origin/mini.

### Phase 0: Recover task state
**Status:** complete
- Root planning documents copied into this pinned plan at user clarification.
- Branch mini at 97ca0d6; three existing worktrees; no active hcom workers.
- T0 stale-worktree pruning was already complete; preserve the remaining task worktrees.

### Phase 1: Accept shutdown and fuzz work
**Status:** complete
- Review committed fixes and uncommitted shutdown configuration/tests.
- Reproduce cancellation regression coverage and incremental fuzz boundaries.
- Integrate accepted work into mini without overwriting worktree changes.

### Phase 2: Accept target filtering
**Status:** complete
- Review existing uncommitted target-filter implementation and tests.
- Enforce host allowlist and public-IP policy with DNS rebinding-safe connections.
- Document settings and security model; verify denial and opt-out behavior.

### Phase 3: Combined validation and handoff
**Status:** complete
- Run pytest, Ruff lint/format, pyright, ty and local TCP smoke test.
- Review final changes and repository state; record residual risks.
- Root plan requests commit and push: commit only after checks; inspect configured upstream before push.

## Next Step
No implementation work remains. Hand off verified changes on origin/mini; preserve the pre-existing root planning edits and retained worktrees.

## Decisions made
- User clarified that root documents describe the incomplete task; recover them into an explicitly pinned new plan instead of silently selecting an unrelated named plan.
- Preserve root documents and uncommitted worker output; do not reset or remove worktrees.

## Errors encountered
| Error | Attempt | Resolution |
|---|---|---|
| Plan resolver refused unpinned multi-plan repository | 1 | User clarified root task; initialized and pinned 2026-11-07-resume-root-proxy-enhancements. |

| Target connector edit rejected | 1-2 | Used exact current signature with unique context; applied successfully. |
| DNS tests returned 400 | 1 | Added required Host header to CONNECT requests; 81 target tests passed. |
| Pyright sockaddr union indexing | 1 | Narrowed tuple by length; type checks passed. |
| hcom send rejected positional syntax | 1 | Read CLI send help. |


| Target patch config overlap | 1 | Applied non-config hunks; explicitly combined shutdown and target fields. |
| Combined shutdown tests rejected loopback | 1 | Add explicit allow_private_networks=True to local-only test server configs. |
| Regression edit context changed by formatter | 1 | Read exact formatted blocks and reapplied targeted edits. |

| ty deprecated contextmanager annotations | 1 | Used Generator return annotations; ty clean. |
| pytest reserved request parameter | 1 | Renamed to wire_request; 20 cleanup tests pass. |
