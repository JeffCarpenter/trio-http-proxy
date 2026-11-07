# Findings

## Requirements
User requests branch micro and discerning choice of nano or mini; prior handoff describes smaller CONNECT-only nano based on mini.

## Research findings
- nano f87d9b9 directly descends from mini c812a7b; 452 runtime source lines, baseline just ci passes Ruff, Pyright, 92 tests.
- mini contains settings, destination filtering, custom DNS, limiting, custom shutdown. nano already removes these and adds bounded handshake writes; nano is the better base.
- streaming.py is unchanged from mini: unbounded pipe callback hooks plus wrapper closures, mutable phase and mutable deadline state (89 lines). No tests or runtime consumers need callbacks beyond that wrapper.
- Current EOF handling is inside send phase; the refactor must continue bounding send_eof and suppress only BrokenResourceError/ClosedResourceError there.
- Keep h11 and parser unchanged; existing fuzz tests verify incremental headers, size boundaries, early bytes, authority validation.

## Technical decisions
One pipe with keyword-only idle_timeout and send_timeout, bounded defaults of 300 and 30 seconds. Operation-local trio.fail_after scopes replace the callback closures, mutable phase, and deadline nudging. Both send_all and send_eof remain bounded. Remove pipe_with_timeout, callback arguments, custom timeout messages, and per-chunk logging rather than compatibility aliases; document the Python API migration. Infinity explicitly disables either budget. Runtime source drops from 452 to 400 lines; streaming.py drops from 89 to 38.

## Threat model
Untrusted/slow clients and stalled destinations must not turn reduced code into unbounded reads, writes, or leaked streams. Bind loopback by default. No authentication or SSRF protection; private destinations remain reachable. Do not remove parser size bounds or cleanup on cancellation.

## Issues encountered
One incorrect skill path corrected; no project baseline failures. Initial Pyright validation of new tests rejected dictionary-unpacked float kwargs as potentially supplying the integer bufmaxsz argument; replaced unpacking with explicit keyword calls. Ruff requested reformatting after the shorter function name; applied formatter.

## Final verification
- just ci passed: Ruff clean, Pyright clean, 106 tests passed (92 original + 14 new cases).
- ruff format --check and git diff --check passed.
- Coverage: streaming 100% statement/branch; whole runtime 95%. Remaining gaps are unchanged parser guards, async context branches, and subprocess module entry point.
- Ten consecutive loopback TCP suite runs passed (50 tests total).
- Console-script and module CLI --help both passed and report Micro with unchanged flags/defaults.
- Protocol parser, HTTP constants, exception hierarchy, and uv.lock have no changes.
- Reviewed lifecycle paths: every receive/send/EOF budget is fresh; outer cancellation is not converted into a timeout; pipe retains caller ownership; stalled writes/half-close cancel siblings and close client/upstream under http_proxy.
- Branch ancestry verified: micro was created at nano f87d9b9; mini c812a7b is an ancestor.
- User authorized committing after verification. Pre-commit formatting, lint, Pyright, all 106 tests, and whitespace checks passed again. No push authorized.

## Useful paths
src/trio_http_proxy/streaming.py; proxy.py; __init__.py; tests/test_invariants.py; docs/configuration.md.
