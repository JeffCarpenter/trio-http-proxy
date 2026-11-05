from functools import partial

import pytest
import trio


async def _dest_http_server(stream: trio.SocketStream) -> None:
    """Reply 200 OK to a single request, echoing the request line as the body."""
    request = await stream.receive_some(4096)
    body = request.split(b"\r\n", 1)[0] if request else b""
    await stream.send_all(
        b"HTTP/1.1 200 OK\r\n"
        b"Content-Length: %d\r\n"
        b"Connection: close\r\n"
        b"\r\n%s" % (len(body), body)
    )


@pytest.fixture
async def dest_server_port(nursery: trio.Nursery) -> int:
    """Run a minimal HTTP server in the background for the test's duration.

    Follows pytest-trio's "Running a background server from a fixture" pattern:
    nursery.start() blocks until the server signals readiness via
    task_status.started(), here returning the OS-assigned listening port.
    """
    listeners = await nursery.start(partial(trio.serve_tcp, _dest_http_server, 0))
    return listeners[0].socket.getsockname()[1]
