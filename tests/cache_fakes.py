"""Controllable backend composed with a real in-memory cache, not patched methods."""

from collections.abc import Callable
from typing import Any

from cashews import Cache

from trio_http_proxy.dependencies import CacheBackend


class ControlledBackend:
    """Effects may be changed to model outages/recovery while the cache is live."""

    def __init__(self, storage: CacheBackend | None = None) -> None:
        self.storage: CacheBackend = (
            storage if storage is not None else Cache(name="test-backend")
        )
        self.effects: dict[str, Callable[..., Any]] = {}
        # Appended on the bridge thread; inspect after awaiting the operation.
        self.operations: list[str] = []

    def setup(self, settings_url: str, *, suppress: bool) -> object:
        setup = self.effects.get("setup", self.storage.setup)
        return setup(settings_url, suppress=suppress)

    async def _call(self, operation: str, *args: Any, **kwargs: Any) -> Any:
        self.operations.append(operation)
        effect = self.effects.get(operation)
        if effect is not None:
            return await effect(*args, **kwargs)
        return await getattr(self.storage, operation)(*args, **kwargs)

    async def ping(self) -> bytes:
        return await self._call("ping")

    async def get(self, key: str, default: Any = None) -> Any:
        return await self._call("get", key, default=default)

    async def set(self, key: str, value: Any, *, expire: float | None = None) -> object:
        return await self._call("set", key, value, expire=expire)

    async def incr(self, key: str, value: int = 1, expire: float | None = None) -> int:
        return await self._call("incr", key, value=value, expire=expire)

    async def get_expire(self, key: str) -> int:
        return await self._call("get_expire", key)

    async def expire(self, key: str, timeout: float) -> object:
        return await self._call("expire", key, timeout)

    async def delete_match(self, pattern: str) -> None:
        await self._call("delete_match", pattern)

    async def clear(self) -> None:
        await self._call("clear")

    async def close(self) -> None:
        await self._call("close")
