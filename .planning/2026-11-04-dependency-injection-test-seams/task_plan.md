# Task Plan: dependency injection test seams

## Goal
Remove collaborator monkeypatches using small typed, optional injection seams; preserve runtime defaults and behavior.

## Current Phase
Complete

## Next Step
None. Ready for user review.

## Phases
### Phase 1: Requirements & Discovery
- [x] Inventory monkeypatches and direct method replacement.
- [x] Establish baseline: 287 tests passed.
- **Status:** complete

### Phase 2: Design
- [x] Select cache backend, host resolver, recovery clock, bridge thread factory.
- [x] Use structural protocols and keyword-only defaults; preserve configuration API.
- **Status:** complete

### Phase 3: Implementation
- [x] Add injection to ProxyCache and ProxyCacheBuilder.
- [x] Migrate fault/recovery/startup/DNS tests and behavioral shutdown coverage.
- [x] Add isolation and property coverage and dependency documentation.
- **Status:** complete

### Phase 4: Testing & Verification
- [x] Review diff and cached DNS semantics; verify core edited modules/tests with mcpls.
- [x] Run pytest, pyright, and scoped Ruff.
- **Status:** complete

### Phase 5: Delivery
- [x] Record final outcomes and residual risks.
- **Status:** complete

## Decisions
- Keep environment patches: these test actual environment loading.
- Cashews Cache structurally satisfies the public-operation backend protocol.
- Replace Cashews DNS decorator with equivalent get/set to avoid putting a Cashews decorator API into the backend contract.
- Backend lifecycle is owned by the cache; injected clocks affect only backend recovery, not global time or TTLs.

## Errors Encountered
- Cached mcpls imports did not discover the new dependencies module; restarted only the python LSP.
- Async generator first-iteration must happen on the bridge loop for its shutdown hooks to own finalization; behavioral test corrected accordingly.
- Recovery property test initially omitted a positive rate-limit cap; corrected the test precondition rather than altering existing rate-limit defaults.
