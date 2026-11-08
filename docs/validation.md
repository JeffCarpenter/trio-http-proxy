# Validation

Run from the repository root after `uv sync --locked`:

```bash
just ci
just mutation
```

`just ci` runs formatting/lint, the Sans-io import boundary, the no-monkeypatch
policy, Pyright, and one pytest invocation with coverage. `just mutation` is
separate: it runs selected tests once per mutant rather than repeating them in
the ordinary CI gate.

Individual checks:

```bash
uv run --locked ruff format --check
uv run --locked ruff check
uv run --locked pyright
uv run --locked pytest --cov --cov-report=term-missing
uv run --locked python scripts/check_no_monkeypatch.py tests
uv run --locked python scripts/verify_mutations.py
```

## Coverage

The gate requires 100% line and branch coverage of `src/trio_http_proxy`.
CLI tests merge coverage from real `python -I -m trio_http_proxy` subprocesses.
Only static-only `TYPE_CHECKING` imports and the impossible false branch of
`if not TYPE_CHECKING` are exempt. Do not exclude runtime paths to satisfy the
threshold. Verification scripts have focused regression tests and are included
in Pyright; the source coverage threshold does not apply to them.

## No-monkeypatch policy

Tests must use explicit dependency injection and purpose-built fake streams.
The AST checker scans Python files under `tests`, reporting paths and lines for:

- `unittest.mock.patch` APIs, including imported/simple aliases;
- `monkeypatch`, `pytest.MonkeyPatch`, and common `mocker.patch`/`mocker.spy` APIs;
- writes/deletions on imported dependencies, including simple aliases,
  `setattr`/`delattr`, and dictionary writes through the imported object;
- replacement of stream operations (`send_all`, `receive_some`, `send_eof`,
  `aclose`), or assignment of a local function/lambda as an object's behavior.

Comments and strings are not violations. State changes on local fake objects,
normal fake method definitions, and constructor/call-site injection are allowed.
Creating a `Mock` alone is not a patch. Imported objects and their instances are
conservatively treated as dependencies; prefer local fakes and explicit injection
rather than mutating imported helpers.

This is a focused review guard, not whole-program analysis or a security sandbox.
Dynamic reflection, container-carried aliases, wildcard imports, and complex
rebinding can escape its analysis. Review those patterns manually rather than
assuming a clean scan proves their absence. Snippet tests never execute prohibited
code; they pass source strings to the checker.

## Mutation verification

The verifier copies source into temporary directories, confirms that subprocesses
import those copies, and runs a passing unmodified control before any mutants.
It does not edit the working source. Exact-match drift checks and compilation
prevent stale mutation definitions from silently passing.

There are 16 independent mutants: target/framing/size guards, each of five invalid
timeout operands, the EOF callback guard, best-effort error replies, each of three
`bufmaxsz` forwarding sites, and CLI port validation. A kill requires a pytest
assertion failure (including an expected exception not being raised). Survivors,
collection errors, crashes, skipped controls, and timeouts fail verification.
Each pytest run has a short timeout; output identifies the mutant and selected
test. Import-origin and result checks use explicit failures, so Python's `-O` and
`PYTHONOPTIMIZE` cannot disable them. Regression tests exercise both optimized modes
with invalid controls, survivors, crashes, missing reports, and wrong import origins.
When source changes, update the catalog and its regression tests deliberately.

## LSP diagnostics versus CLI Pyright

Use locked CLI Pyright as the reproducible type-check gate. A live editor/mcpls
server may use a different host-installed version or stale indexing state. Before
changing valid code to silence a live-only diagnostic:

1. Compare `uv run --locked pyright --verbose` with the server executable/version,
   workspace root, and dependency paths from go-to-definition.
2. Refresh only the affected Python server and recheck after indexing completes.
   Pending, evicted, indexing, or degraded diagnostic responses are inconclusive.
3. If the mismatch remains, retain the diagnostic code/range, configuration, and
   smallest import pair as a tooling reproduction. Do not add speculative ignores.

The previously reported `.streams` import diagnostic did not reproduce during
this follow-up, either before or after a scoped refresh. Both import definitions
and dependency resolution were correct. No configuration fix was demonstrated.
