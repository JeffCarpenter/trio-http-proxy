"""Lifecycle state machine for ProxyCache."""

import enum


class LifecycleState(enum.Enum):
    """Lifecycle states of a ProxyCache instance."""

    NEW = "NEW"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"


__all__ = ["LifecycleState"]
