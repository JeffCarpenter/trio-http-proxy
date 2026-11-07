"""Sync tests for the sans-io core: no trio runtime, no fake streams."""

import ast
import importlib.util
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

import trio_http_proxy
from trio_http_proxy import http
from trio_http_proxy.errors import ProxyProtocolError, RequestTooLargeError
from trio_http_proxy.protocol import (
    ConnectParser,
    ConnectRequest,
    error_response,
)
from trio_http_proxy.timeouts import PipeTimeouts

CONNECT = b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n"

PUBLIC_NAMES = (
    "ConnectParser",
    "ConnectRequest",
    "PipeTimeouts",
    "ProxyError",
    "ProxyProtocolError",
    "RequestTooLargeError",
    "error_response",
    "errors",
    "http",
    "http_proxy",
    "main",
    "parse_connect_target",
    "pipe",
    "pipe_with_timeout",
    "process_as_connect_request",
    "protocol",
    "proxy",
    "start_server",
    "streaming",
    "timeouts",
    "tunnel",
)

# Prepended to child-interpreter code: any import of trio or trio.* raises.
BLOCK_TRIO = """
import importlib.abc
import sys


class BlockTrio(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "trio" or fullname.startswith("trio."):
            raise ImportError(f"trio import blocked: {fullname}")
        return None


sys.meta_path.insert(0, BlockTrio())
"""


def run_isolated(
    code: str, *, block_trio: bool = False
) -> subprocess.CompletedProcess[str]:
    """Run code in a fresh isolated interpreter so import state is pristine."""
    source = (BLOCK_TRIO if block_trio else "") + textwrap.dedent(code)
    return subprocess.run(
        [sys.executable, "-I", "-c", source],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


@pytest.mark.parametrize("module", ["protocol", "timeouts", "errors", "http"])
def test_sansio_modules_do_not_import_trio(module: str) -> None:
    spec = importlib.util.find_spec(f"trio_http_proxy.{module}")
    assert spec is not None and spec.origin is not None
    tree = ast.parse(Path(spec.origin).read_text())
    imported = {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module and node.level == 0
    }
    assert "trio" not in imported


def test_sansio_modules_import_and_parse_with_trio_blocked() -> None:
    result = run_isolated(
        r"""
        try:
            import trio
        except ImportError:
            pass
        else:
            raise SystemExit("blocker inert: trio imported")

        import trio_http_proxy.protocol
        import trio_http_proxy.timeouts
        import trio_http_proxy.errors
        import trio_http_proxy.http

        loaded = sorted(m for m in sys.modules if m.split(".")[0] == "trio")
        assert "trio" not in sys.modules, loaded

        from trio_http_proxy.protocol import ConnectParser, ConnectRequest

        parser = ConnectParser()
        parser.receive_data(
            b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n"
        )
        assert parser.next_request() == ConnectRequest("example.com", 443, b"")
        """,
        block_trio=True,
    )
    assert result.returncode == 0, result.stderr


def test_package_exports_are_lazy_and_load_trio_only_on_demand() -> None:
    result = run_isolated(
        """
        import sys

        import trio_http_proxy

        missing = set(trio_http_proxy.__all__) - set(dir(trio_http_proxy))
        assert not missing, missing
        assert "trio" not in sys.modules

        from trio_http_proxy import ConnectParser, PipeTimeouts, errors, http
        from trio_http_proxy import error_response, protocol, timeouts
        assert "trio" not in sys.modules

        from trio_http_proxy import http_proxy, start_server, tunnel
        assert "trio" in sys.modules
        assert "http_proxy" in vars(trio_http_proxy)
        assert trio_http_proxy.http_proxy is trio_http_proxy.proxy.http_proxy
        assert trio_http_proxy.pipe is trio_http_proxy.streaming.pipe
        assert trio_http_proxy.main is trio_http_proxy.proxy.main
        """
    )
    assert result.returncode == 0, result.stderr


def test_module_entry_point_still_runs() -> None:
    result = subprocess.run(
        [sys.executable, "-I", "-m", "trio_http_proxy", "--help"],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "Nano HTTP CONNECT proxy" in result.stdout


@pytest.mark.parametrize("name", PUBLIC_NAMES)
def test_every_public_name_resolves_and_is_listed(name: str) -> None:
    # No dir() check here: getattr caches the name into globals(), so it could not fail.
    assert getattr(trio_http_proxy, name) is not None
    assert name in trio_http_proxy.__all__


@pytest.mark.parametrize(
    "submodule", ["errors", "http", "protocol", "proxy", "streaming", "timeouts"]
)
def test_submodule_attribute_resolves_in_a_fresh_interpreter(submodule: str) -> None:
    """Package.submodule must work before anything has imported that submodule.

    An in-process test cannot see this: other tests have already imported every
    submodule, which binds the attribute and bypasses the package __getattr__.
    """
    result = run_isolated(
        f"""
        import sys

        import trio_http_proxy

        assert "trio_http_proxy.{submodule}" not in sys.modules
        module = getattr(trio_http_proxy, {submodule!r})
        assert module.__name__ == "trio_http_proxy.{submodule}", module
        """
    )
    assert result.returncode == 0, result.stderr


def test_all_matches_documented_public_names() -> None:
    assert sorted(trio_http_proxy.__all__) == sorted(PUBLIC_NAMES)


def test_unknown_attribute_raises_attribute_error() -> None:
    missing = "nope"
    with pytest.raises(AttributeError, match="has no attribute 'nope'"):
        getattr(trio_http_proxy, missing)
    assert not hasattr(trio_http_proxy, "__wrapped__")


def test_parser_needs_data_then_returns_request() -> None:
    parser = ConnectParser()
    assert parser.next_request() is None
    parser.receive_data(CONNECT)
    assert parser.next_request() == ConnectRequest("example.com", 443, b"")


def test_parser_is_chunk_split_invariant() -> None:
    parser = ConnectParser()
    for i in range(len(CONNECT) - 1):
        parser.receive_data(CONNECT[i : i + 1])
        assert parser.next_request() is None
    parser.receive_data(CONNECT[-1:])
    assert parser.next_request() == ("example.com", 443, b"")


def test_parser_keeps_trailing_bytes_and_is_idempotent() -> None:
    parser = ConnectParser()
    parser.receive_data(CONNECT + b"\x16\x03\x01early")
    first = parser.next_request()
    assert first == ("example.com", 443, b"\x16\x03\x01early")
    assert parser.next_request() is first


def test_parser_accepts_bytearray_and_memoryview() -> None:
    parser = ConnectParser()
    parser.receive_data(bytearray(CONNECT[:10]))
    parser.receive_data(memoryview(CONNECT[10:]))
    assert parser.next_request() == ("example.com", 443, b"")


def test_parser_ignores_empty_data_instead_of_signalling_eof() -> None:
    parser = ConnectParser()
    parser.receive_data(b"")
    parser.receive_data(CONNECT)
    assert parser.next_request() is not None


def test_parser_eof_before_any_data_is_protocol_error() -> None:
    parser = ConnectParser()
    assert parser.next_request() is None
    parser.receive_eof()
    with pytest.raises(ProxyProtocolError):
        parser.next_request()


def test_parser_eof_mid_headers_is_protocol_error() -> None:
    parser = ConnectParser()
    parser.receive_data(CONNECT[:-2])
    assert parser.next_request() is None
    parser.receive_eof()
    with pytest.raises(ProxyProtocolError):
        parser.next_request()


def test_parser_rejects_non_connect_method() -> None:
    parser = ConnectParser()
    parser.receive_data(b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
    with pytest.raises(ProxyProtocolError, match="Expected CONNECT"):
        parser.next_request()


def test_parser_rejects_bad_target() -> None:
    parser = ConnectParser()
    parser.receive_data(b"CONNECT user@example.com:443 HTTP/1.1\r\nHost: x\r\n\r\n")
    with pytest.raises(ProxyProtocolError):
        parser.next_request()


def test_parser_raises_too_large_without_terminator() -> None:
    parser = ConnectParser(maxreqsz=64)
    parser.receive_data(b"CONNECT example.com:443 HTTP/1.1\r\nX: " + b"a" * 100)
    with pytest.raises(RequestTooLargeError):
        parser.next_request()


def test_parser_headers_exactly_at_limit_pass_and_one_less_fails() -> None:
    parser = ConnectParser(maxreqsz=len(CONNECT))
    parser.receive_data(CONNECT)
    assert parser.next_request() is not None

    parser = ConnectParser(maxreqsz=len(CONNECT) - 1)
    parser.receive_data(CONNECT)
    with pytest.raises(RequestTooLargeError):
        parser.next_request()


def test_parser_trailing_bytes_do_not_count_toward_limit() -> None:
    parser = ConnectParser(maxreqsz=len(CONNECT))
    parser.receive_data(CONNECT + b"x" * 500)
    request = parser.next_request()
    assert request is not None
    assert request.trailing_data == b"x" * 500


def test_error_response_maps_protocol_errors() -> None:
    assert error_response(RequestTooLargeError("big")) == http.HTTP_414_URI_TOO_LONG
    assert error_response(ProxyProtocolError("bad")) == http.HTTP_400_BAD_REQUEST


def test_pipe_timeouts_walks_idle_send_idle() -> None:
    policy = PipeTimeouts(idle_timeout=300.0, send_timeout=30.0, now=100.0)
    assert (policy.phase, policy.deadline) == ("idle", 400.0)
    assert (policy.reason, policy.timeout) == ("idle", 300.0)

    assert policy.received(150.0) == 180.0
    assert (policy.phase, policy.reason, policy.timeout) == (
        "send",
        "send stalled",
        30.0,
    )

    assert policy.sent(160.0) == 460.0
    assert (policy.phase, policy.reason, policy.timeout) == ("idle", "idle", 300.0)


def _unterminated_request(length: int) -> bytes:
    """A CONNECT request cut off at length bytes, before its blank line."""
    prefix = CONNECT[:-2] + b"X: " + b"a" * length
    assert b"\r\n\r\n" not in prefix[:length]
    return prefix[:length]


def test_incomplete_headers_at_exactly_maxreqsz_are_rejected_at_once() -> None:
    parser = ConnectParser(maxreqsz=60)
    parser.receive_data(_unterminated_request(60))
    with pytest.raises(RequestTooLargeError):
        parser.next_request()


def test_incomplete_headers_one_byte_under_maxreqsz_still_wait_for_data() -> None:
    parser = ConnectParser(maxreqsz=60)
    parser.receive_data(_unterminated_request(59))
    assert parser.next_request() is None
