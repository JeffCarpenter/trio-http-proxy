"""RateLimitError exception definition."""

from typing import Any

import cashews


class RateLimitError(cashews.RateLimitError):
    """Rate-limit rejection with an optional HTTP Retry-After value."""

    def __init__(
        self,
        *args: Any,
        retry_after: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.retry_after = retry_after

    @property
    def retry_after_seconds(self) -> float | None:
        return float(self.retry_after) if self.retry_after is not None else None


__all__ = ["RateLimitError"]
