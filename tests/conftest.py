from functools import partial

import pytest
import trio
from cache_fakes import ControlledBackend


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


# --- Configuration model fixtures -------------------------------------------------

from trio_http_proxy.cache import ProxyCache, ProxyCacheBuilder
from trio_http_proxy.config import (
    Settings,
    get_settings,
    reset_settings_cache,
)
from trio_http_proxy.proxy import reset_conn_limiter

_CONFIG_ENV_VARS = frozenset(
    name.upper()
    for field in Settings.model_fields
    for name in (field, "TRIO_HTTP_PROXY_" + field)
)


@pytest.fixture(autouse=True)
def _clean_config_env(monkeypatch):
    """Isolate every test from ambient proxy configuration.

    Removes all legacy and TRIO_HTTP_PROXY_* config env vars and disables
    .env loading for freshly built Settings (via the `settings` fixtures below),
    so tests start from field defaults unless they set values explicitly.
    """
    for var in _CONFIG_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    reset_settings_cache()
    reset_conn_limiter()
    yield
    reset_settings_cache()
    reset_conn_limiter()


@pytest.fixture
def backend() -> ControlledBackend:
    """An instance-local backend whose public effects model outages."""
    return ControlledBackend()


@pytest.fixture
def backend_factory() -> type[ControlledBackend]:
    return ControlledBackend


@pytest.fixture
def settings() -> Settings:
    """Fresh Settings (env isolated by _clean_config_env)."""
    return Settings()


@pytest.fixture
def proxy_cache_builder() -> ProxyCacheBuilder:
    """A fresh ProxyCacheBuilder per test."""
    return ProxyCacheBuilder()


@pytest.fixture
def default_proxy_cache() -> ProxyCache:
    """A fresh ProxyCache per test; avoids singleton state leaking across tests."""
    return ProxyCacheBuilder().build()


@pytest.fixture
def isolated_get_settings():
    """get_settings() with automatic cache invalidation around the test."""
    return get_settings
