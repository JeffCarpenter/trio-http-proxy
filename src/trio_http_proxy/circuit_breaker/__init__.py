"""Circuit breaker implementation for destination-based fault isolation."""

from .circuit_breaker_manager import (
    CircuitBreakerManager,
    canonicalize_destination,
)
from .circuit_permit import CircuitPermit
from .circuit_state import CircuitState
from .destination_circuit_breaker import DestinationCircuitBreaker

__all__ = [
    "CircuitBreakerManager",
    "CircuitPermit",
    "CircuitState",
    "DestinationCircuitBreaker",
    "canonicalize_destination",
]
