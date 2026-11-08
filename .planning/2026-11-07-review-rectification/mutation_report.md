# Guard mutation verification

Run from the repository root:

```sh
uv run --locked python .planning/2026-11-07-review-rectification/verify_mutations.py
```

Result: **10/10 killed by assertions**, no survivors, collection errors, crashes,
or timeouts. The final post-migration run took 0.38–0.44 seconds per mutant,
including interpreter startup. Selected tests also passed against an unmodified
scratch-copy control. All ten kills remain assertion-based after removing patches.

The verifier creates one source copy per mutant in a temporary directory, sets
`PYTHONPATH` to that copy, and checks the imported package's origin in the same
interpreter that runs pytest. It checks replacement counts and compiles each
mutant before testing. Only pytest exit status 1 with a JUnit assertion failure
(`AssertionError` or `DID NOT RAISE`) counts as a kill; watchdog/pytest timeouts do
not. Scratch sources and reports are removed on exit. The checkout is never
mutated.

| # | Removed/reverted guard | Assertion that kills it |
|---|---|---|
| 1 | ASCII controls/whitespace | `test_parse_connect_target_invalid` |
| 2 | Backslash/percent delimiters | `test_parse_connect_target_invalid` |
| 3 | IDNA host validation | `test_parse_connect_target_invalid` |
| 4 | CONNECT framing headers | `test_parser_rejects_framing_headers_on_connect` |
| 5 | Positive parser size | `test_parser_rejects_nonpositive_maxreqsz` |
| 6 | Nonnegative/non-NaN timeout values | `test_pipe_timeouts_rejects_invalid_parameters` |
| 7 | Do not call on_receive at EOF | `test_pipe_does_not_call_on_receive_on_eof` |
| 8 | Best-effort error reply | `test_error_reply_suppresses_transport_failure` |
| 9 | All three bufmaxsz forwarding sites | `test_http_proxy_forwards_bufmaxsz` |
| 10 | CLI port range | `test_main_rejects_out_of_range_port` |

Two test changes make the evidence assertion-based and fast:

- Error-reply suppression is checked directly with a fault-injecting SendStream,
  including BrokenResourceError, ClosedResourceError, and OSError. A leaked
  exception is captured and rejected by an explicit assertion.
- The existing invalid-port test uses an inert runner if validation disappears,
  so a missing guard immediately yields `DID NOT RAISE`, rather than binding a
  real port or blocking. It now passes explicit argv and runner arguments to
  main(), with no monkeypatch or global dependency replacement.

These are the ten grouped mutants specified by next_steps.yaml, not an exhaustive
mutation score for every individual clause (for example each forwarding site or
NaN operand separately).
