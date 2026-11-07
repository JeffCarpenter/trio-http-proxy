"""Bounded CONNECT fuzz tests for authority parsing, chunking, and size limits.

Chunking must preserve the destination and all tunnel bytes, including bytes
left unread in the stream; the returned initial bytes alone may vary by split.
"""

from __future__ import annotations

from typing import Final

import pytest
import trio
from hypothesis import given, settings
from hypothesis import strategies as st

from trio_http_proxy.config import ProxyConfig
from trio_http_proxy.errors import ProxyProtocolError, RequestTooLargeError
from trio_http_proxy.protocol import parse_connect_target, process_as_connect_request

# ---------------------------------------------------------------------------
# Test Stream Helpers
# ---------------------------------------------------------------------------


class ChunkedReceiveStream(trio.abc.ReceiveStream):
    """An in-memory Trio ReceiveStream that yields predetermined byte chunks."""

    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks: list[bytes] = list(chunks)
        self._closed: bool = False

    async def receive_some(self, max_bytes: int | None = None) -> bytes:
        await trio.lowlevel.checkpoint()
        if self._closed:
            raise trio.ClosedResourceError("Stream is closed")
        if not self._chunks:
            return b""
        chunk = self._chunks.pop(0)
        limit = max_bytes if max_bytes is not None else len(chunk)
        if len(chunk) <= limit:
            return chunk
        # If the chunk is larger than requested, return limit and retain remainder
        self._chunks.insert(0, chunk[limit:])
        return chunk[:limit]

    async def aclose(self) -> None:
        self._closed = True


def partition_bytes(data: bytes, chunk_sizes: list[int]) -> list[bytes]:
    """Partition bytes into chunks according to a list of chunk sizes."""
    if not data:
        return []
    chunks: list[bytes] = []
    idx = 0
    for size in chunk_sizes:
        if idx >= len(data):
            break
        if size <= 0:
            continue
        chunks.append(data[idx : idx + size])
        idx += size
    if idx < len(data):
        chunks.append(data[idx:])
    return [c for c in chunks if c]


async def _read_all_stream(stream: trio.abc.ReceiveStream) -> bytes:
    """Drain all remaining bytes from a ReceiveStream until EOF."""
    collected: list[bytes] = []
    while True:
        data = await stream.receive_some()
        if not data:
            break
        collected.append(bytes(data))
    return b"".join(collected)


# ---------------------------------------------------------------------------
# Strategies for parse_connect_target
# ---------------------------------------------------------------------------

_WHITESPACE_CHARS: Final[str] = " \t\r\n\v\f"
_CONTROL_CHARS: Final[str] = "".join(chr(c) for c in range(32)) + chr(127)

_SAMPLE_HOSTS: Final[list[str]] = [
    "",
    "example.com",
    "localhost",
    "127.0.0.1",
    "::1",
    "[::1]",
    "[2001:db8::1]",
    "[]",
    "[:]",
    "[:443",
    "]:443",
    "[[::1]]",
    "[invalid_ipv6]",
    "user:pass@host",
    "host/path",
    "host?query",
    "host#fragment",
    "exa\x00mple.com",
    "   ",
]

_port_extremes = st.sampled_from(
    [
        "-9999999999999999999999999999",
        "-65536",
        "-1",
        "0",
        "1",
        "80",
        "443",
        "8080",
        "65535",
        "65536",
        "70000",
        "9999999999999999999999999999",
        "not_a_port",
        "",
        " 443",
        "443 ",
        "443\x00",
        "0x10",
        "+443",
    ]
)

_port_strategy = st.one_of(
    _port_extremes,
    st.integers(min_value=-(10**30), max_value=10**30).map(str),
    st.text(max_size=15),
)

_host_strategy = st.one_of(
    st.sampled_from(_SAMPLE_HOSTS),
    st.text(max_size=40),
    st.text(alphabet=_WHITESPACE_CHARS, max_size=15),
    st.text(alphabet=_CONTROL_CHARS, max_size=15),
    st.from_regex(r"\[[a-fA-F0-9:]{0,40}\]", fullmatch=True),
    st.from_regex(r"\[[a-fA-F0-9:]{0,40}:[0-9]{1,10}\]", fullmatch=True),
)

_colon_strategy = st.sampled_from(
    [
        "",
        ":",
        "::",
        ":::",
        " : ",
        ": ",
        " :",
        "\t:\t",
    ]
)

_composite_targets = st.builds(
    lambda h, c, p: f"{h}{c}{p}",
    _host_strategy,
    _colon_strategy,
    _port_strategy,
)

_bracketed_ipv6_targets = st.builds(
    lambda h, p: f"[{h}]:{p}",
    st.text(max_size=30),
    _port_strategy,
)

_target_bytes_strategy = st.one_of(
    st.binary(max_size=150),
    _composite_targets.map(lambda s: s.encode("utf-8", errors="surrogateescape")),
    _composite_targets.map(lambda s: s.encode("latin-1", errors="replace")),
    _composite_targets.map(lambda s: s.encode("ascii", errors="replace")),
    _bracketed_ipv6_targets.map(lambda s: s.encode("utf-8", errors="surrogateescape")),
    _bracketed_ipv6_targets.map(lambda s: s.encode("ascii", errors="replace")),
    st.builds(
        lambda prefix, byte_val, suffix: prefix + bytes([byte_val]) + suffix,
        st.binary(max_size=30),
        st.integers(min_value=0, max_value=255),
        st.binary(max_size=30),
    ),
)


# ---------------------------------------------------------------------------
# Invariant 1: parse_connect_target robustness
# ---------------------------------------------------------------------------


@settings(max_examples=100, derandomize=True, deadline=None)
@given(target=_target_bytes_strategy)
def test_parse_connect_target_invariants(target: bytes) -> None:
    """parse_connect_target must return a valid (host, port) pair or raise ProxyProtocolError.

    Never raises unhandled exceptions such as ValueError, IndexError, AttributeError,
    TypeError, or KeyError.
    """
    try:
        host, port = parse_connect_target(target)
    except ProxyProtocolError:
        # Expected failure mode for malformed/invalid targets
        return

    # Invariant: Must return valid host string and port in 1..65535
    assert isinstance(host, str), f"Host must be str, got {type(host)}"
    assert len(host) > 0, f"Host must not be empty string, target was {target!r}"
    assert 1 <= port <= 65535, (
        f"Port {port} outside valid range [1, 65535] for {target!r}"
    )


# ---------------------------------------------------------------------------
# Strategies for valid HTTP CONNECT requests
# ---------------------------------------------------------------------------

_valid_hostname_labels = st.from_regex(
    r"[a-z0-9]([a-z0-9-]{0,10}[a-z0-9])?", fullmatch=True
)
_valid_domain_strategy = st.lists(_valid_hostname_labels, min_size=1, max_size=4).map(
    lambda labels: ".".join(labels)
)
_valid_ipv4_strategy = st.from_regex(
    r"(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)",
    fullmatch=True,
)
_valid_ipv6_literal_strategy = st.sampled_from(
    [
        "[::1]",
        "[2001:db8::1]",
        "[fe80::1]",
        "[::ffff:192.0.2.1]",
    ]
)

_valid_target_host_strategy = st.one_of(
    _valid_domain_strategy,
    _valid_ipv4_strategy,
    _valid_ipv6_literal_strategy,
)

_valid_port_strategy = st.integers(min_value=1, max_value=65535)

_header_name_strategy = st.from_regex(r"[a-zA-Z0-9_-]{1,24}", fullmatch=True).filter(
    lambda name: name.lower() not in ("host", "content-length", "transfer-encoding", "")
)
_header_value_strategy = st.from_regex(r"[a-zA-Z0-9_ .,;:=+/?!-]{0,64}", fullmatch=True)
_headers_strategy = st.lists(
    st.tuples(_header_name_strategy, _header_value_strategy),
    max_size=6,
)

_body_strategy = st.binary(max_size=256)
_chunk_sizes_strategy = st.lists(
    st.integers(min_value=1, max_value=64),
    min_size=1,
    max_size=32,
)


def _build_connect_request_bytes(
    host: str,
    port: int,
    headers: list[tuple[str, str]],
    body: bytes,
) -> bytes:
    """Serialize a valid HTTP CONNECT request with Host header and optional headers/body."""
    target_str = f"{host}:{port}"
    parts: list[bytes] = [
        f"CONNECT {target_str} HTTP/1.1\r\nHost: {target_str}\r\n".encode("ascii")
    ]
    for key, val in headers:
        parts.append(f"{key}: {val}\r\n".encode("ascii"))
    parts.append(b"\r\n")
    parts.append(body)
    return b"".join(parts)


def _expected_parsed_host(target_host: str) -> str:
    """The parser returns IPv6 hosts without brackets."""
    if target_host.startswith("[") and target_host.endswith("]"):
        return target_host[1:-1]
    return target_host


# ---------------------------------------------------------------------------
# Invariant 2: process_as_connect_request chunk-splitting invariance
# ---------------------------------------------------------------------------


@settings(max_examples=100, derandomize=True, deadline=None)
@given(
    target_host=_valid_target_host_strategy,
    port=_valid_port_strategy,
    headers=_headers_strategy,
    body=_body_strategy,
    chunk_sizes=_chunk_sizes_strategy,
)
def test_process_as_connect_request_chunk_splitting(
    target_host: str,
    port: int,
    headers: list[tuple[str, str]],
    body: bytes,
    chunk_sizes: list[int],
) -> None:
    """Different receive boundaries preserve the destination and total tunnel data."""
    raw_request = _build_connect_request_bytes(target_host, port, headers, body)
    chunks = partition_bytes(raw_request, chunk_sizes)

    expected_host = _expected_parsed_host(target_host)

    async def run_scenario() -> None:
        # Baseline: raw request sent in a single chunk via ChunkedReceiveStream
        rx_base = ChunkedReceiveStream([raw_request])
        base_host, base_port, base_initial_body = await process_as_connect_request(
            rx_base
        )
        base_trailing_remainder = await _read_all_stream(rx_base)
        base_total_body = base_initial_body + base_trailing_remainder

        # Chunked: raw request partitioned into arbitrary chunk sizes
        rx_chunked = ChunkedReceiveStream(chunks)
        chunk_host, chunk_port, chunk_initial_body = await process_as_connect_request(
            rx_chunked
        )
        chunk_trailing_remainder = await _read_all_stream(rx_chunked)
        chunk_total_body = chunk_initial_body + chunk_trailing_remainder

        # Invariant assertions:
        # 1. Output must match baseline exactly
        assert (chunk_host, chunk_port) == (base_host, base_port)
        assert chunk_total_body == base_total_body

        # 2. Target must parse host and port accurately
        assert (chunk_host, chunk_port) == (expected_host, port)
        assert chunk_total_body == body

    trio.run(run_scenario)


@pytest.mark.parametrize("chunk_size", [1, 2, 7])
def test_process_as_connect_request_incremental_stream(chunk_size: int) -> None:
    """Exercise bytewise delivery, split final CRLF, and binary tunnel data."""
    body = b"\x00\xff\r\nCONNECT not-a-request"
    raw_request = _build_connect_request_bytes("[::1]", 443, [], body)
    chunks = [
        raw_request[i : i + chunk_size] for i in range(0, len(raw_request), chunk_size)
    ]

    async def run_scenario() -> None:
        stream = ChunkedReceiveStream(chunks)
        host, port, trailing = await process_as_connect_request(stream)
        assert (host, port) == ("::1", 443)
        assert trailing + await _read_all_stream(stream) == body

    trio.run(run_scenario)


# ---------------------------------------------------------------------------
# Invariant 3: process_as_connect_request oversize invariant
# ---------------------------------------------------------------------------

_max_req_len_strategy = st.integers(min_value=40, max_value=250)
_excess_len_strategy = st.integers(min_value=1, max_value=300)
_oversize_chunk_size_strategy = st.integers(min_value=1, max_value=64)
_overflow_kind_strategy = st.sampled_from(["target", "header_value", "many_headers"])


@settings(max_examples=100, derandomize=True, deadline=None)
@given(
    max_request_len=_max_req_len_strategy,
    excess=_excess_len_strategy,
    chunk_size=_oversize_chunk_size_strategy,
    kind=_overflow_kind_strategy,
)
def test_process_as_connect_request_oversize_invariant(
    max_request_len: int,
    excess: int,
    chunk_size: int,
    kind: str,
) -> None:
    """process_as_connect_request must consistently raise RequestTooLargeError when limits are exceeded.

    Must never crash, hang, or raise unhandled exceptions.
    """
    if kind == "target":
        overflow_host = "a" * (max_request_len + excess)
        payload = f"CONNECT {overflow_host}:443 HTTP/1.1\r\nHost: {overflow_host}:443\r\n\r\n".encode(
            "ascii"
        )
    elif kind == "header_value":
        overflow_val = "v" * (max_request_len + excess)
        payload = f"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\nX-Oversize: {overflow_val}\r\n\r\n".encode(
            "ascii"
        )
    else:  # "many_headers"
        lines: list[str] = [
            "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n"
        ]
        curr_len = sum(len(l) for l in lines)
        header_idx = 0
        while curr_len < (max_request_len + excess):
            line = f"X-Header-{header_idx}: value-{header_idx}\r\n"
            lines.append(line)
            curr_len += len(line)
            header_idx += 1
        lines.append("\r\n")
        payload = "".join(lines).encode("ascii")

    chunks = [payload[i : i + chunk_size] for i in range(0, len(payload), chunk_size)]

    async def run_scenario() -> None:
        cfg = ProxyConfig(maxreqsz=max_request_len)
        rx = ChunkedReceiveStream(chunks)

        with trio.fail_after(2.0), pytest.raises(RequestTooLargeError):
            await process_as_connect_request(rx, config=cfg)

    trio.run(run_scenario)


@settings(max_examples=100, derandomize=True, deadline=None)
@given(
    target_host=_valid_target_host_strategy,
    port=_valid_port_strategy,
    chunk_size=st.sampled_from([1, 2, 7, 8192]),
    body=_body_strategy,
)
def test_process_as_connect_request_boundary_limits(
    target_host: str,
    port: int,
    chunk_size: int,
    body: bytes,
) -> None:
    """Count only headers: their exact size succeeds, one byte less fails."""
    req_bytes = _build_connect_request_bytes(target_host, port, [], b"")
    exact_len = len(req_bytes)
    payload = req_bytes + body
    chunks = [payload[i : i + chunk_size] for i in range(0, len(payload), chunk_size)]
    expected_host = _expected_parsed_host(target_host)

    async def run_boundary_scenario() -> None:
        # At exact boundary: must succeed
        cfg_exact = ProxyConfig(maxreqsz=exact_len)
        rx_exact = ChunkedReceiveStream(chunks)
        host, parsed_port, trailing = await process_as_connect_request(
            rx_exact, config=cfg_exact
        )
        assert (host, parsed_port) == (expected_host, port)
        assert trailing + await _read_all_stream(rx_exact) == body

        # 1 byte under required length: must raise RequestTooLargeError
        cfg_too_small = ProxyConfig(maxreqsz=exact_len - 1)
        rx_too_small = ChunkedReceiveStream(chunks)
        with pytest.raises(RequestTooLargeError):
            await process_as_connect_request(rx_too_small, config=cfg_too_small)

    trio.run(run_boundary_scenario)


@pytest.mark.parametrize("missing_bytes", [1, 2, 3, 4])
def test_process_as_connect_request_truncated_headers(missing_bytes: int) -> None:
    """EOF during the terminating CRLF must not accept an incomplete request."""
    request = _build_connect_request_bytes("example.com", 443, [], b"")

    async def run_scenario() -> None:
        stream = ChunkedReceiveStream(
            [bytes([byte]) for byte in request[:-missing_bytes]]
        )
        with trio.fail_after(2.0), pytest.raises(ProxyProtocolError):
            await process_as_connect_request(stream)

    trio.run(run_scenario)
