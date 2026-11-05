"""CircuitPermit tokenized lease for circuit breaker admission control."""

from __future__ import annotations

from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from .destination_circuit_breaker import DestinationCircuitBreaker


class CircuitPermit:
    """Tokenized lease representing admission into a destination circuit breaker."""

    def __init__(
        self,
        breaker: DestinationCircuitBreaker,
        epoch: int,
        permit_id: int,
        is_probe: bool = False,
    ) -> None:
        self.breaker = breaker
        self.epoch = epoch
        self.permit_id = permit_id
        self.is_probe = is_probe
        self.was_aborted: bool = False
        self.was_completed: bool = False

    def abort(self) -> None:
        """Abort this lease, releasing any probe reservation immediately."""
        if self.was_aborted or self.was_completed:
            return
        self.was_aborted = True
        self.breaker._abort_permit(self)

    def mark_completed(self) -> bool:
        if self.was_aborted or self.was_completed:
            return False
        self.was_completed = True
        return True

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        self.abort()


__all__ = ["CircuitPermit"]
