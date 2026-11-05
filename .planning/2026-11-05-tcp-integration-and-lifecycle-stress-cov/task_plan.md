# TCP integration, lifecycle stress, and branch coverage

## Goal
Persist the loopback TCP smoke test, extend behavioral lifecycle stress coverage, and report full-suite branch coverage without replacing application methods or adding external network dependencies.

## Steps
- [completed] audit: Inspect integration/stress tests, bridge cancellation/shutdown contracts, and baseline checks.
- [completed] tcp: Add permanent real-TCP CONNECT, binary forwarding, DNS reuse, peer-IP quota, and shutdown coverage.
- [completed] stress: Exercise connection churn, handshake/forwarding cancellation, pending backend work, concurrent shutdown, and outage recovery under load.
- [completed] verify: Repeat focused tests, run CI and branch coverage, inspect gaps, and record results.
- [completed] commit: Review final changes and repeat CI before the user-authorized commit.

## Design constraints
- Bind IPv4 loopback on ephemeral ports; no Internet, TLS toolchain, mocks, or application patches.
- Use real ProxyCache/Cashews, explicit backend/resolver adapters, and event-gated interleavings.
- Assert owned resources (handlers, sockets, limiter tokens, bridge loop/thread, pending tasks), not global process counters.
- Bound tests with generous deadlines; avoid sleeps as race synchronization.
- Preserve production behavior unless a new test exposes an actionable bug.

## Validation
`just ci`; repeated new integration/stress tests; full pytest with `--cov-branch`, terminal/JSON/HTML reports; `git diff --check`.
