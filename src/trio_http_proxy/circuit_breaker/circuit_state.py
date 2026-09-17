"""Circuit breaker state representation."""

import enum


class CircuitState(str, enum.Enum):
    """Enumeration of circuit breaker operational states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


__all__ = ["CircuitState"]
