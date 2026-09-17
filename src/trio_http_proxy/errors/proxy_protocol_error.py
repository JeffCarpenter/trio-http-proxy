"""ProxyProtocolError exception for HTTP proxy protocol violations."""


class ProxyProtocolError(ValueError):
    """The client's CONNECT request is malformed or violates the
    authority-form contract (RFC 7231 section 4.3.6). Raised in place of
    `assert` so validation survives `python -O` (asserts are stripped under
    optimization, silently turning every one of these checks into a no-op)."""


__all__ = ["ProxyProtocolError"]
