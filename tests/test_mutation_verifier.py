"""Regression tests for mutation construction and assertion-only results."""

import ast
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.verify_mutations import (
    MUTATIONS,
    PACKAGE,
    apply_mutation,
    assertion_killed,
    control_passed,
)


def report(body: str) -> str:
    return f'<testsuites><testsuite tests="1"><testcase name="sample">{body}</testcase></testsuite></testsuites>'


@pytest.mark.parametrize(
    ("returncode", "xml", "expected"),
    [
        (1, report('<failure message="assert 1 == 2">AssertionError</failure>'), True),
        (
            1,
            report(
                '<failure message="Failed: DID NOT RAISE &lt;class ValueError&gt;"/>'
            ),
            True,
        ),
        (
            1,
            report(
                '<failure message="Failed: DID NOT RAISE ValueError">idle_timeout = -1</failure>'
            ),
            True,
        ),
        (0, report(""), False),
        (2, report('<error message="collection error"/>'), False),
        (1, report('<failure message="NameError">NameError: missing</failure>'), False),
        (
            1,
            report('<failure message="Timeout">AssertionError: timed out</failure>'),
            False,
        ),
        (
            1,
            report('<failure message="ValueError">assert value\nValueError</failure>'),
            False,
        ),
        (
            1,
            report(
                '<failure message="ValueError">AssertionError in source comment</failure>'
            ),
            False,
        ),
        (1, report('<failure message="assert False"/><error message="oops"/>'), False),
        (1, report('<skipped message="not run"/>'), False),
        (1, "not xml", False),
        (1, "<testsuites/>", False),
    ],
)
def test_only_assertions_kill_mutants(
    returncode: int, xml: str, expected: bool
) -> None:
    assert assertion_killed(returncode, xml) is expected


@pytest.mark.parametrize(
    ("returncode", "xml", "expected"),
    [
        (0, report(""), True),
        (1, report(""), False),
        (0, report('<skipped message="skip"/>'), False),
        (0, report('<error message="error"/>'), False),
        (0, report('<failure message="assert False"/>'), False),
        (0, "<testsuites/>", False),
        (0, "broken", False),
    ],
)
def test_control_requires_executed_passing_tests(
    returncode: int, xml: str, expected: bool
) -> None:
    assert control_passed(returncode, xml) is expected


def test_catalog_is_granular_and_all_mutations_compile() -> None:
    assert len(MUTATIONS) == 16
    assert len({mutation.name for mutation in MUTATIONS}) == 16
    forwarding = [m for m in MUTATIONS if m.name.endswith("bufmaxsz")]
    timeouts = [m for m in MUTATIONS if m.name.startswith("valid-timeouts-")]
    assert len(forwarding) == 3
    assert len(timeouts) == 5
    for mutation in MUTATIONS:
        original = (PACKAGE / mutation.module).read_text()
        changed = apply_mutation(original, mutation)
        assert changed != original
        ast.parse(changed)
        if mutation in forwarding:
            assert original.count("bufmaxsz=bufmaxsz") == 3
            assert changed.count("bufmaxsz=bufmaxsz") == 2
        if mutation in timeouts:
            assert changed.count("False") == original.count("False") + 1


def test_drift_fails_instead_of_running_an_unmodified_mutant() -> None:
    with pytest.raises(ValueError, match="source has drifted"):
        apply_mutation("# source changed", MUTATIONS[0])


def optimized_process(code: str, mode: str) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    env.pop("PYTHONOPTIMIZE", None)
    flags = ["-O"] if mode == "flag" else []
    if mode == "environment":
        env["PYTHONOPTIMIZE"] = "1"
    return subprocess.run(
        [sys.executable, *flags, "-c", code],
        cwd=PACKAGE.parent.parent,
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )


@pytest.mark.parametrize("mode", ["flag", "environment"])
def test_optimized_runner_rejects_wrong_import_origin(
    tmp_path: Path, mode: str
) -> None:
    source = tmp_path / "empty-source"
    source.mkdir()
    result = optimized_process(
        "from pathlib import Path\n"
        "from scripts.verify_mutations import run_tests\n"
        f"result = run_tests(Path({str(source)!r}), "
        f"Path({str(tmp_path / 'report.xml')!r}), "
        "['tests/test_mutation_verifier.py::test_drift_fails_instead_of_running_an_unmodified_mutant'])\n"
        "print(result.stdout, result.stderr)\n"
        "raise SystemExit(result.returncode)\n",
        mode,
    )
    assert result.returncode != 0, result.stdout + result.stderr
    assert "outside isolated source" in result.stdout + result.stderr
    assert not (tmp_path / "report.xml").exists()


@pytest.mark.parametrize("mode", ["flag", "environment"])
@pytest.mark.parametrize(
    ("scenario", "diagnostic"),
    [
        ("control", "Control failed"),
        ("survivor", "Mutant not killed"),
        ("missing-report", "Mutant report missing"),
        ("crash", "Mutant not killed"),
    ],
)
def test_optimized_main_rejects_invalid_results(
    mode: str, scenario: str, diagnostic: str
) -> None:
    # Explicit effect injection: no replacement of module members or globals.
    result = optimized_process(
        "import subprocess\n"
        "from scripts.verify_mutations import main\n"
        f"scenario = {scenario!r}\n"
        "def runner(source, report, tests):\n"
        "    is_control = source.name == 'control'\n"
        "    if not is_control and scenario == 'missing-report':\n"
        "        return subprocess.CompletedProcess([], 1, '', 'no report')\n"
        "    failed_control = is_control and scenario == 'control'\n"
        "    crashed = not is_control and scenario == 'crash'\n"
        "    body = '<error message=\"crash\"/>' if failed_control or crashed else ''\n"
        "    report.write_text('<testsuites><testsuite><testcase name=\"probe\">'\n"
        "                      + body + '</testcase></testsuite></testsuites>')\n"
        "    return subprocess.CompletedProcess([], 1 if failed_control or crashed else 0, '', '')\n"
        "main(runner=runner)\n",
        mode,
    )
    assert result.returncode != 0, result.stdout + result.stderr
    assert diagnostic in result.stderr, result.stdout + result.stderr
    assert "16/16" not in result.stdout
    assert "KILLED" not in result.stdout
