"""Verify the ten b13f8bd guard mutants without touching the working source.

Run from the repository root:
    uv run --locked python .planning/2026-11-07-review-rectification/verify_mutations.py

Each subprocess imports an isolated source copy via PYTHONPATH, verifies that
import's origin, then runs selected real tests. Only assertion failures count as
kills; collection errors, crashes, survivors and timeouts fail this verifier.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = ROOT / "src" / "trio_http_proxy"


@dataclass(frozen=True)
class Mutation:
    name: str
    module: str
    replacements: tuple[tuple[str, str, int], ...]
    test: str


MUTATIONS = (
    Mutation(
        "ascii-control-whitespace",
        "protocol.py",
        (
            (
                (
                    "    if any(ord(c) <= 0x20 or ord(c) >= 0x7F for c in decoded_target):\n"
                    '        raise ProxyProtocolError(f"{target!r} contains control or whitespace octets")\n'
                ),
                "",
                1,
            ),
        ),
        "tests/test_protocol.py::test_parse_connect_target_invalid",
    ),
    Mutation(
        "backslash-and-percent",
        "protocol.py",
        (('("@", "?", "#", "/", "\\\\", "%")', '("@", "?", "#", "/")', 1),),
        "tests/test_protocol.py::test_parse_connect_target_invalid",
    ),
    Mutation(
        "idna-host-validation",
        "protocol.py",
        (
            (
                (
                    '    if not decoded_target.startswith("["):\n'
                    "        try:\n"
                    '            desthost.encode("idna")\n'
                    "        except UnicodeError as exc:\n"
                    '            raise ProxyProtocolError(f"{target!r} has an invalid host") from exc\n'
                ),
                "",
                1,
            ),
        ),
        "tests/test_protocol.py::test_parse_connect_target_invalid",
    ),
    Mutation(
        "connect-framing",
        "protocol.py",
        (
            (
                (
                    "        if any(\n"
                    '            name in (b"content-length", b"transfer-encoding")\n'
                    "            for name, _ in event.headers\n"
                    "        ):\n"
                    '            raise ProxyProtocolError("CONNECT request must not contain framing headers")\n'
                ),
                "",
                1,
            ),
        ),
        "tests/test_sansio.py::test_parser_rejects_framing_headers_on_connect",
    ),
    Mutation(
        "positive-maxreqsz",
        "protocol.py",
        (
            (
                (
                    "        if maxreqsz < 1:\n"
                    '            raise ValueError(f"maxreqsz must be >= 1, got {maxreqsz}")\n'
                ),
                "",
                1,
            ),
        ),
        "tests/test_sansio.py::test_parser_rejects_nonpositive_maxreqsz",
    ),
    Mutation(
        "valid-timeouts",
        "timeouts.py",
        (
            (
                (
                    "        if (\n"
                    "            idle_timeout < 0\n"
                    "            or send_timeout < 0\n"
                    "            or math.isnan(idle_timeout)\n"
                    "            or math.isnan(send_timeout)\n"
                    "            or math.isnan(now)\n"
                    "        ):\n"
                    '            raise ValueError("idle_timeout, send_timeout, and now must be valid")\n'
                ),
                "",
                1,
            ),
        ),
        "tests/test_sansio.py::test_pipe_timeouts_rejects_invalid_parameters",
    ),
    Mutation(
        "receive-callback-on-eof",
        "streaming.py",
        (
            (
                (
                    "        if not chunk:\n"
                    "            break\n"
                    "        if on_receive is not None:\n"
                    "            on_receive()\n"
                ),
                (
                    "        if on_receive is not None:\n"
                    "            on_receive()\n"
                    "        if not chunk:\n"
                    "            break\n"
                ),
                1,
            ),
        ),
        "tests/test_pipe_deadlines.py::test_pipe_does_not_call_on_receive_on_eof",
    ),
    Mutation(
        "best-effort-error-reply",
        "proxy.py",
        (
            (
                (
                    "        try:\n"
                    "            await stream.send_all(response)\n"
                    "        except trio.BrokenResourceError, trio.ClosedResourceError, OSError:\n"
                    "            pass\n"
                ),
                "        await stream.send_all(response)\n",
                1,
            ),
        ),
        "tests/test_tunnel_cleanup.py::test_error_reply_suppresses_transport_failure",
    ),
    Mutation(
        "bufmaxsz-forwarding",
        "proxy.py",
        (("            bufmaxsz=bufmaxsz,\n", "", 3),),
        "tests/test_tunnel_forwarding.py::test_http_proxy_forwards_bufmaxsz",
    ),
    Mutation(
        "cli-port-range",
        "proxy.py",
        (
            (
                (
                    "    if not (0 <= args.port <= 65535):\n"
                    '        parser.error(f"argument --port: must be between 0 and 65535, got {args.port}")\n'
                ),
                "",
                1,
            ),
        ),
        "tests/test_cli.py::test_main_rejects_out_of_range_port",
    ),
)


def run_tests(
    source: Path, report: Path, tests: list[str]
) -> subprocess.CompletedProcess[str]:
    env = {
        **os.environ,
        "PYTHONPATH": str(source),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTEST_ADDOPTS": "",
    }
    # Verify the actual pytest interpreter, not just a separate import probe.
    bootstrap = (
        "import pathlib, sys, trio_http_proxy; "
        "assert pathlib.Path(trio_http_proxy.__file__).is_relative_to(sys.argv.pop(1)); "
        "import pytest; raise SystemExit(pytest.main(sys.argv[1:]))"
    )
    return subprocess.run(
        [
            sys.executable,
            "-c",
            bootstrap,
            str(source),
            "-q",
            "--maxfail=1",
            "--timeout=5",
            "--import-mode=importlib",
            "-p",
            "no:cacheprovider",
            f"--junitxml={report}",
            *tests,
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="trio-proxy-mutants-") as directory:
        scratch = Path(directory)
        control = scratch / "control"
        shutil.copytree(
            PACKAGE,
            control / PACKAGE.name,
            ignore=shutil.ignore_patterns("__pycache__"),
        )
        tests = list(dict.fromkeys(mutation.test for mutation in MUTATIONS))
        result = run_tests(control, scratch / "control.xml", tests)
        assert result.returncode == 0, (
            f"Control failed:\n{result.stdout}\n{result.stderr}"
        )
        print(
            "CONTROL: selected tests pass against an unmodified scratch copy",
            flush=True,
        )

        for mutation in MUTATIONS:
            source = scratch / mutation.name
            shutil.copytree(control, source)
            module = source / PACKAGE.name / mutation.module
            code = module.read_text()
            for old, new, count in mutation.replacements:
                assert code.count(old) == count, f"{mutation.name}: source has drifted"
                code = code.replace(old, new)
            compile(code, str(module), "exec")
            module.write_text(code)
            report = scratch / f"{mutation.name}.xml"
            started = time.monotonic()
            result = run_tests(source, report, [mutation.test])
            elapsed = time.monotonic() - started
            evidence = f"{mutation.name}:\n{result.stdout}\n{result.stderr}"
            assert result.returncode == 1, evidence
            suite = ET.parse(report)
            assert not suite.findall(".//error"), evidence
            failures = suite.findall(".//failure")
            assert failures, evidence
            for failure in failures:
                message = "".join(failure.itertext()) + failure.get("message", "")
                assert "Timeout" not in message and "timed out" not in message, evidence
                assert "AssertionError" in message or "DID NOT RAISE" in message, (
                    evidence
                )
            print(
                f"KILLED {mutation.name}: assertion in {mutation.test} ({elapsed:.2f}s)",
                flush=True,
            )

    print(
        f"{len(MUTATIONS)}/{len(MUTATIONS)} mutants killed by assertions; no timeouts"
    )


if __name__ == "__main__":
    main()
