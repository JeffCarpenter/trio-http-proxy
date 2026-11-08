"""Verify individual guard mutants without touching the working source.

Run from the repository root with ``just mutation`` or:
    uv run --locked python scripts/verify_mutations.py

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
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
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
    *(
        Mutation(
            f"valid-timeouts-{name}",
            "timeouts.py",
            ((operand, "False", 1),),
            "tests/test_sansio.py::test_pipe_timeouts_rejects_invalid_parameters",
        )
        for name, operand in (
            ("negative-idle", "idle_timeout < 0"),
            ("negative-send", "send_timeout < 0"),
            ("nan-idle", "math.isnan(idle_timeout)"),
            ("nan-send", "math.isnan(send_timeout)"),
            ("nan-now", "math.isnan(now)"),
        )
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
    *(
        Mutation(
            f"{name}-bufmaxsz",
            "proxy.py",
            ((context + line, context, 1),),
            "tests/test_tunnel_forwarding.py::test_http_proxy_forwards_bufmaxsz",
        )
        for name, context, line in (
            (
                "tunnel-to-parser",
                "await process_as_connect_request(\n            client_stream,\n",
                "            bufmaxsz=bufmaxsz,\n",
            ),
            (
                "proxy-to-tunnel",
                "await tunnel(\n                client_stream,\n",
                "                bufmaxsz=bufmaxsz,\n",
            ),
            (
                "proxy-to-pipes",
                "                                target,\n",
                "                                bufmaxsz=bufmaxsz,\n",
            ),
        )
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


def apply_mutation(code: str, mutation: Mutation) -> str:
    """Apply exact edits; source drift must not silently create a survivor."""
    for old, new, count in mutation.replacements:
        if code.count(old) != count:
            raise ValueError(f"{mutation.name}: source has drifted")
        code = code.replace(old, new)
    compile(code, mutation.module, "exec")
    return code


def test_cases(report: str) -> tuple[ET.Element, ...]:
    try:
        return tuple(ET.fromstring(report).iter("testcase"))
    except ET.ParseError:
        return ()


def control_passed(returncode: int, report: str) -> bool:
    cases = test_cases(report)
    return returncode == 0 and bool(cases) and all(not list(case) for case in cases)


def assertion_killed(returncode: int, report: str) -> bool:
    """Accept pytest assertions, never errors or assertion text in a traceback."""
    cases = test_cases(report)
    if returncode != 1 or not cases:
        return False
    failures = [failure for case in cases for failure in case.findall("failure")]
    if not failures or any(
        case.find("error") is not None or case.find("skipped") is not None
        for case in cases
    ):
        return False
    for failure in failures:
        message = failure.get("message", "").lstrip()
        evidence = message + "".join(failure.itertext())
        if "Timeout" in evidence or "timed out" in evidence.lower():
            return False
        if not message.startswith(
            ("assert ", "AssertionError", "Failed: DID NOT RAISE")
        ):
            return False
    return True


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
        "import pathlib, sys, trio_http_proxy\n"
        "source = pathlib.Path(sys.argv.pop(1)).resolve()\n"
        "origin = pathlib.Path(trio_http_proxy.__file__).resolve()\n"
        "if not origin.is_relative_to(source):\n"
        "    raise RuntimeError(f'Import outside isolated source: {origin} not in {source}')\n"
        "import pytest\n"
        "raise SystemExit(pytest.main(sys.argv[1:]))\n"
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


def main(
    *,
    runner: Callable[
        [Path, Path, list[str]], subprocess.CompletedProcess[str]
    ] = run_tests,
) -> None:
    with tempfile.TemporaryDirectory(prefix="trio-proxy-mutants-") as directory:
        scratch = Path(directory)
        control = scratch / "control"
        shutil.copytree(
            PACKAGE,
            control / PACKAGE.name,
            ignore=shutil.ignore_patterns("__pycache__"),
        )
        tests = list(dict.fromkeys(mutation.test for mutation in MUTATIONS))
        control_report = scratch / "control.xml"
        result = runner(control, control_report, tests)
        if not control_report.is_file():
            raise RuntimeError(
                f"Control failed: report missing\n{result.stdout}\n{result.stderr}"
            )
        if not control_passed(result.returncode, control_report.read_text()):
            raise RuntimeError(f"Control failed:\n{result.stdout}\n{result.stderr}")
        print(
            "CONTROL: selected tests pass against an unmodified scratch copy",
            flush=True,
        )

        for mutation in MUTATIONS:
            source = scratch / mutation.name
            shutil.copytree(control, source)
            module = source / PACKAGE.name / mutation.module
            module.write_text(apply_mutation(module.read_text(), mutation))
            report = scratch / f"{mutation.name}.xml"
            started = time.monotonic()
            result = runner(source, report, [mutation.test])
            elapsed = time.monotonic() - started
            evidence = f"{mutation.name}:\n{result.stdout}\n{result.stderr}"
            if not report.is_file():
                raise RuntimeError(f"Mutant report missing: {evidence}")
            if not assertion_killed(result.returncode, report.read_text()):
                raise RuntimeError(f"Mutant not killed by assertions: {evidence}")
            print(
                f"KILLED {mutation.name}: assertion in {mutation.test} ({elapsed:.2f}s)",
                flush=True,
            )

    print(
        f"{len(MUTATIONS)}/{len(MUTATIONS)} mutants killed by assertions; no timeouts"
    )


if __name__ == "__main__":
    main()
