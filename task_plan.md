# Task Plan: Enhancements (Items 5, 2, 3, 4) via Hierarchical Delegation

## Goal
Implement graceful signal shutdown (Item 2), Hypothesis property-based fuzzing (Item 3), target allowlist / SSRF guard (Item 4), and prune stale worktrees (Item 5) using strict hierarchical delegation, bounded worker pool, disjoint scopes, and structured concurrency.

## Current Phase
Complete. Implementation and acceptance are recorded in
`.planning/2026-11-07-resume-root-proxy-enhancements/`.

## Roles
- Coordinator: owns decomposition, scheduling, scope-conflict detection, acceptance, and final whole-repo checks.
- Workers: bounded pool (`active_workers <= 5`), disjoint write scopes, no sub-delegation.

## Tranches
- [x] Tranche T0 (Item 5: Prune Stale Worktrees)
  - Scope: Worktree paths `.worktrees/*`
  - Deps: []
  - Status: DONE (pruned and verified)
- [x] Tranche T1 (Item 2: Graceful Signal Shutdown)
  - Scope: `src/trio_http_proxy/config.py`, `src/trio_http_proxy/proxy.py`, `tests/test_shutdown.py`
  - Deps: [T0]
  - Status: ACCEPTED; cancellation deadlock fixed, signal setup fallback narrowed, and parent-cancellation/cleanup regressions passed.
- [x] Tranche T2 (Item 3: Hypothesis Property-Based Fuzzing)
  - Scope: `tests/test_fuzz_protocol.py`
  - Deps: [T0]
  - Status: ACCEPTED; real incremental streams, exact size boundaries, tunnel-byte preservation, and bounded deterministic Hypothesis coverage verified.
- [x] Tranche T3 (Item 4: Target Allowlist & SSRF Guard)
  - Scope: `src/trio_http_proxy/config.py`, `src/trio_http_proxy/http.py`, `src/trio_http_proxy/protocol.py`, `src/trio_http_proxy/proxy.py`, `tests/test_target_filter.py`
  - Deps: [T1] (serialized due to overlapping scope on `config.py` and `proxy.py`)
  - Status: ACCEPTED; host/port policy, DNS-answer vetting, numeric connection racing, scope/NAT64 identity checks, and failed-handshake cleanup verified.
- [x] Final Combined Checks & Acceptance
  - `uv run pytest`: 217 passed; coverage run: 98%.
  - `ruff check` and `ruff format --check`: passed.
  - `pyright` and `ty check`: passed.
  - Live CLI smoke: default denial, private opt-out, early data, echo, and SIGTERM draining passed.
  - Implementation and accepted plan committed and pushed through `3eaaba4` on `origin/mini`.

## Execution Log
- [x] Coordinator: Decomposed tranches and analyzed scope conflicts.
- [x] Batch 0: Executed T0 (Prune Stale Worktrees). `git worktree list` verified clean.
- [x] Batch 1: Dispatched T1 (`.worktrees/shutdown`) and T2 (`.worktrees/fuzz`) concurrently (`active_workers = 2 <= 5`).
- [x] Review: Dispatched `pi` Reviewers (`lilo` for T1, `nemo` for T2) via `hcom`. High-quality findings recorded in `findings.md`.
- [x] Resolved T1/T2 review findings, integrated their accepted artifacts, and completed T3 with independent security review.
- [x] Retired workers; retained task worktrees to preserve accepted artifacts.

## Next Step
No work remains. Root planning documents reflect the accepted implementation and verification results.
