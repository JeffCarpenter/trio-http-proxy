"""Circuit breaker implementation for destination-based fault isolation."""

from .circuit_breaker_manager import (
    CircuitBreakerManager,
    canonicalize_destination,
)
from .circuit_permit import CircuitPermit
from .destination_circuit_breaker import (
    CircuitState,
    DestinationCircuitBreaker,
)

__all__ = [
    "CircuitBreakerManager",
    "CircuitPermit",
    "CircuitState",
    "DestinationCircuitBreaker",
    "canonicalize_destination",
]
