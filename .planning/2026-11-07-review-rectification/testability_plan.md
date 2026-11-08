# Monkeypatch-free testability

Status: complete. User-approved implementation delivered. The user subsequently authorized a local commit after final verification; no push requested.

## Delivered

- `main(argv=None, *, runner=trio.run)` is the only new production seam.
  CLI and module-entry tests use explicit inputs, real subprocesses and public
  CoverageData merging rather than patches. Default invocation is unchanged.
- `tests/streams.py` provides a typed half-closeable recording wrapper with one
  optional send hook. Three test modules reuse it for read limits, chunk counts,
  successful writes, close calls and cancellation. No stream methods are replaced.
- Failed-handshake tests observe one upstream close before raw-resource teardown;
  a successful-handshake test verifies early bytes and caller ownership.
- Existing port-0 integration tests cover normal forwarding and default binding.
  The cancellation test also checks a distinct loopback host, readiness, complete
  CONNECT response, closed listeners and both tunnel endpoints before teardown.
  All six integration tests passed ten consecutive runs without sleeps.
- Final gates: `just ci` passes 212 tests; all eight source modules retain 100%
  line/branch coverage (279 statements, 68 branches); ten scratch-copy mutants
  die by assertions (0.38–0.44 seconds). Source/AST audit of 13 test/support
  modules finds no patch APIs or manual dependency-method replacement.
- mcpls diagnostics were refreshed for all 12 affected Python files: nine clean;
  three tests report unresolved new `.streams` imports only in the live server.
  Fresh CLI Pyright checks source and tests with zero errors. No diagnostic
  suppressions or import-path workarounds were added.

The sections below preserve the approved design and pre-migration inventory.

## Goal

Reach a test suite that never monkeypatches. Add small, explicit dependency seams
where the current tests reveal a need. Prefer passing real interface
implementations to replacing globals or methods. Do not add a dependency
container, settings object, or another optional runtime layer.

## 1. CLI arguments and runner — highest ROI

**Location:** `src/trio_http_proxy/proxy.py:main`; `tests/test_cli.py`.

Today the CLI tests patch `sys.argv`, `trio.run`, environment variables, and the
module-entry `main` reference. Make argv and the async runner explicit inputs to
`main`, with defaults preserving normal `main()` invocation. Keep CLI parsing
before invoking the runner. Use a precisely typed callable for the runner; no
service object is needed.

Tests can then pass an argument list and a recording, interrupting, or
must-not-run callable. A missing port-validation guard must cause an immediate
assertion failure, not start a server. Preserve host/port defaults, accepted port
0, argparse exit status 2 for invalid ports, and KeyboardInterrupt handling.

Test the environment-ignored contract with a subprocess and an explicit `env`
mapping rather than mutating process-global environment variables. Test the real
module entry with subprocess `--help` and invalid-port invocations. Test the
non-entry import path in-process without replacing `main`. Preserve entry-point
coverage via supported subprocess coverage collection; do not suppress the
entry-point code just to eliminate a patch.

**Acceptance:** no monkeypatch fixture in `tests/test_cli.py`; unchanged default
entry points, parsing behavior, and error messages; port-range mutant still dies
by a fast assertion.

## 2. Stream behavior belongs in stream implementations

**Locations:** `tests/test_tunnel_cleanup.py`, `tests/test_tunnel_forwarding.py`;
existing `TrackedStream` in `tests/test_invariants.py`.

Replace assignments to individual streams' `send_all` and `receive_some` methods
with small recording/fault-injecting implementations passed to `tunnel` and
`http_proxy`. Compose wrappers with `trio.testing` memory streams for successful
I/O. Use separate failure/gating behavior where needed; avoid a giant fake
controlled by unrelated flags.

Record read limits, writes, close calls, and cancellation at the interface.
Inject stream failures or a send gate through construction. Preserve Trio
checkpoints and receive-size contracts. Advertise HalfCloseableStream only when
the wrapped stream supports it: a wrapper must not change EOF behavior.

The new `FailingReplyStream` test and nursery property show the direction without
requiring any production signature changes. Reuse existing helpers when their
contracts match; share them only once multiple tests need the same behavior.

**Acceptance:** no stream-method patching in the cleanup/forwarding tests;
early bytes, send deadlines, both EOF directions, and sibling cancellation remain
observable; bufmaxsz and EOF mutants still fail assertions.

## 3. Observe cleanup at the owned resource

**Location:** `tests/test_tunnel_cleanup.py:test_failed_handshake_closes_upstream`.

Replace patching of `trio.aclose_forcefully` with an upstream wrapper that records
its own `aclose()` call and delegates to the real stream. Assert the observation
before test-fixture teardown can close it. Keep production forceful cleanup real,
including cancellation behavior; do not inject a generic close function just to
verify which helper was called.

**Acceptance:** failure before ownership transfer closes the upstream exactly
once through the production path; success transfers ownership; cancellation
cleanup remains bounded; no global Trio patch.

## 4. Keep and reuse the connector seam

**Location:** `proxy.py:tunnel` and `http_proxy` already accept `_connect`.

Use that existing callable for recording destinations, returning controlled
upstreams, and raising dial errors. Do not add a second network factory or rename
the argument merely for consistency. Keep failed-handshake upstream ownership
and the caller's ownership of successful connections explicit.

**Acceptance:** all connection-setup tests use supplied connectors or loopback
connections, with no global network patches or external DNS dependency.

## 5. Test the server boundary rather than inject its one-line delegation

**Location:** `tests/test_proxy.py:test_start_server_delegates_to_serve_tcp`;
`tests/test_proxy_tcp_integration.py`.

Replace the patched `trio.serve_tcp` delegation assertion with coverage from a
real `start_server(port=0)` loopback test, using nursery.start/task_status to get
listeners and synchronize readiness. Check the bound address and an actual
CONNECT exchange, then cancel the nursery and verify cleanup. Reuse the existing
integration coverage where it already proves the contract.

**Acceptance:** no serve_tcp patch; no fixed-port race or sleeps; normal startup,
readiness, and cancellation remain tested. No new production DI seam is needed.

## Already adequate — leave alone

- `ConnectParser` is sans-I/O and accepts bytes and limits explicitly.
- `PipeTimeouts` accepts the caller's time; do not add another clock abstraction.
- Async deadline tests can use Trio's supported MockClock. Do not thread generic
  clock/cancel-scope factories through the production stack.
- Real stream protocols and the existing connector cover most effect boundaries.

## Migration and completion gate

Implement CLI first, then stream wrappers and cleanup observations, then replace
the delegation test. Use mcpls references and call hierarchy before changing
signatures; check published diagnostics afterward. Keep these as small changes,
not one broad test rewrite.

For each step, run `just ci`, source branch coverage, and the scratch-copy mutant
verifier. Update mutation patterns only when source intentionally changes, not
the expected assertion requirements. Final target: no monkeypatch, mock.patch, or
manual reassignment of dependency methods anywhere in tests; preserve coverage,
all ten assertion-based mutation kills, and structured-concurrency invariants.
A simple source scan can flag regressions, but review interface-level behavior
rather than treating the absence of a keyword as proof.
