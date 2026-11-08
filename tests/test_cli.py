"""The nano CLI only accepts host and port; environment settings are ignored."""

import os
import runpy
import subprocess
import sys
from collections.abc import Awaitable, Callable, Sequence
from functools import partial
from pathlib import Path

import pytest
from coverage import Coverage, CoverageData

from trio_http_proxy import proxy


def run_module(
    args: Sequence[str], data_file: Path
) -> subprocess.CompletedProcess[str]:
    """Measure the real entry point explicitly, without patching subprocesses."""
    current = Coverage.current()
    command = [sys.executable, "-I", "-m"]
    if current is not None:
        command += ["coverage", "run", f"--data-file={data_file}"]
        if current.get_data().has_arcs():
            command.append("--branch")
        command.append("-m")
    result = subprocess.run(
        [*command, "trio_http_proxy", *args],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    if current is not None:
        child_data = CoverageData(basename=str(data_file))
        child_data.read()
        current.get_data().update(child_data)
    return result


def test_importing_module_entry_does_not_start_server() -> None:
    namespace = runpy.run_module(
        "trio_http_proxy", run_name="trio_http_proxy.imported_entry"
    )
    assert namespace["main"] is proxy.main
    assert namespace["__name__"] == "trio_http_proxy.imported_entry"


def test_module_entry_help(tmp_path: Path) -> None:
    result = run_module(["--help"], tmp_path / "help.coverage")
    assert result.returncode == 0
    assert "--host" in result.stdout
    assert "--port" in result.stdout


def test_module_entry_rejects_invalid_port(tmp_path: Path) -> None:
    result = run_module(["--port", "65536"], tmp_path / "port.coverage")
    assert result.returncode == 2
    assert "must be between 0 and 65535" in result.stderr


@pytest.mark.parametrize(
    ("args", "host", "port"),
    [
        ([], "127.0.0.1", 8080),
        (["--host", "::1", "--port", "9090"], "::1", 9090),
        (["--port", "0"], "127.0.0.1", 0),
    ],
)
def test_main_passes_listen_options(args: list[str], host: str, port: int) -> None:
    calls: list[Callable[[], Awaitable[None]]] = []
    proxy.main(args, runner=calls.append)

    assert len(calls) == 1
    target = calls[0]
    assert isinstance(target, partial)
    assert target.func is proxy.start_server
    assert target.args == (port,)
    assert target.keywords == {"host": host}


def test_main_ignores_environment_settings() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            (
                "from trio_http_proxy.proxy import main; "
                "main([], runner=lambda target: print(target.args[0], target.keywords['host']))"
            ),
        ],
        env={
            **os.environ,
            "TRIO_HTTP_PROXY_PORT": "not-an-integer",
            "PORT": "not-an-integer",
        },
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "8080 127.0.0.1"


def test_main_handles_keyboard_interrupt() -> None:
    def interrupt(target: Callable[[], Awaitable[None]]) -> None:
        raise KeyboardInterrupt

    proxy.main([], runner=interrupt)


@pytest.mark.parametrize("bad_port", ["-1", "65536", "70000"])
def test_main_rejects_out_of_range_port(
    bad_port: str, capsys: pytest.CaptureFixture[str]
) -> None:
    calls: list[Callable[[], Awaitable[None]]] = []
    # If validation disappears, the inert runner keeps the failure immediate.
    with pytest.raises(SystemExit) as exc_info:
        proxy.main(["--port", bad_port], runner=calls.append)
    assert exc_info.value.code == 2
    assert "must be between 0 and 65535" in capsys.readouterr().err
    assert calls == []
