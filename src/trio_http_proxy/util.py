def _retry_after_seconds(period: str) -> int:
    if period.endswith("s"):
        try:
            return int(period[:-1])
        except ValueError:
            pass
    try:
        return int(period)
    except ValueError:
        return 60


def redact_url(url: str) -> str:
    """Sanitize userinfo credentials in a URL for safe logging and exceptions."""
    if not url or not url.strip():
        return url
    try:
        from urllib.parse import urlsplit, urlunsplit

        parts = urlsplit(url)
        if parts.password is not None:
            user = parts.username or ""
            netloc_parts = parts.netloc.rsplit("@", 1)
            if len(netloc_parts) == 2:
                host_port = netloc_parts[1]
                netloc = f"{user}:***@{host_port}"
                return urlunsplit(
                    (parts.scheme, netloc, parts.path, parts.query, parts.fragment)
                )
        return url
    except ValueError, AttributeError:
        return url
