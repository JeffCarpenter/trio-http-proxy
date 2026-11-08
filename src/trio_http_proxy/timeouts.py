"""Sans-io idle/send deadline policy for one piped stream direction."""

from __future__ import annotations

import math
from typing import Literal


class PipeTimeouts:
    """Track the idle/send deadline of one pipe. The caller supplies the clock.

    Starts idle. received() means data arrived and must now be sent; sent()
    means that send finished and the pipe is idle again. Both return the new
    absolute deadline.
    """

    __slots__ = ("deadline", "idle_timeout", "phase", "send_timeout")

    def __init__(self, *, idle_timeout: float, send_timeout: float, now: float) -> None:
        if (
            idle_timeout < 0
            or send_timeout < 0
            or math.isnan(idle_timeout)
            or math.isnan(send_timeout)
            or math.isnan(now)
        ):
            raise ValueError("idle_timeout, send_timeout, and now must be valid")
        self.idle_timeout = idle_timeout
        self.send_timeout = send_timeout
        self.phase: Literal["idle", "send"] = "idle"
        self.deadline = now + idle_timeout

    def received(self, now: float) -> float:
        self.phase = "send"
        self.deadline = now + self.send_timeout
        return self.deadline

    def sent(self, now: float) -> float:
        self.phase = "idle"
        self.deadline = now + self.idle_timeout
        return self.deadline

    @property
    def timeout(self) -> float:
        """The timeout that applies in the current phase."""
        return self.send_timeout if self.phase == "send" else self.idle_timeout

    @property
    def reason(self) -> str:
        return "send stalled" if self.phase == "send" else "idle"


__all__ = ["PipeTimeouts"]
