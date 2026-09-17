"""CacheBackendUnavailable exception definition."""


class CacheBackendUnavailable(RuntimeError):
    """The configured cache backend could not service an operation."""


__all__ = ["CacheBackendUnavailable"]
