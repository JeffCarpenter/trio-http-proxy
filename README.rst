trio-http-proxy — nano
======================

A small HTTP CONNECT proxy built on Trio and h11. Requires Python 3.14+.

See `README.md <README.md>`_ for the current usage, security boundaries,
architecture, and development commands.

Quick start::

    uv sync
    uv run trio_http_proxy --port 8080

The default listen address is 127.0.0.1. Environment configuration is not
supported. There is no authentication or destination filtering; do not expose
the proxy to untrusted clients.
