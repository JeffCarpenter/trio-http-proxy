"""Fluent builder API for ProxyCache and Proxy configuration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Self

from .settings import Settings

if TYPE_CHECKING:
    from ..proxy import ProxyBuilder
    from ..proxy_cache import ProxyCache


class SettingsBuilderMixin:
    """Shared `with_settings`/`build_settings` for builders holding
    `_base_settings` and `_overrides`."""

    _base_settings: Settings | None
    _overrides: dict[str, Any]

    def with_settings(self, settings: Settings) -> Self:
        self._base_settings = settings
        return self

    def build_settings(self) -> Settings:
        if self._base_settings is not None:
            return Settings.model_validate(
                {**self._base_settings.model_dump(), **self._overrides}
            )
        return Settings(**self._overrides)


class ProxyCacheBuilder(SettingsBuilderMixin):
    """Fluent builder for constructing and configuring ProxyCache instances.

    Encapsulates cache backend, circuit breaker, and rate limiting configuration
    on top of the consolidated Settings data model.
    """

    def __init__(self, settings: Settings | None = None, **overrides: Any) -> None:
        self._base_settings: Settings | None = settings
        self._overrides: dict[str, Any] = dict(overrides)
        self._built_cache: ProxyCache | None = None

    def with_cache(
        self,
        cache_url: str | None = None,
        *,
        cache_enabled: bool | None = None,
        cache_namespace: str | None = None,
        dns_cache_ttl: str | None = None,
        cache_failure_policy: Literal["fail-open", "fail-closed"] | str | None = None,
        probe_interval: float | None = None,
        startup_timeout: float | None = None,
    ) -> Self:
        """Configure cache backend and lifecycle options."""
        if cache_url is not None:
            self._overrides["cache_url"] = cache_url
        if cache_enabled is not None:
            self._overrides["cache_enabled"] = cache_enabled
        if cache_namespace is not None:
            self._overrides["cache_namespace"] = cache_namespace
        if dns_cache_ttl is not None:
            self._overrides["dns_cache_ttl"] = dns_cache_ttl
        if cache_failure_policy is not None:
            self._overrides["cache_failure_policy"] = cache_failure_policy
        if probe_interval is not None:
            self._overrides["probe_interval"] = probe_interval
        if startup_timeout is not None:
            self._overrides["startup_timeout"] = startup_timeout
        return self

    def with_cache_url(self, cache_url: str) -> Self:
        self._overrides["cache_url"] = cache_url
        return self

    def with_cache_enabled(self, cache_enabled: bool) -> Self:
        self._overrides["cache_enabled"] = cache_enabled
        return self

    def with_namespace(self, cache_namespace: str) -> Self:
        self._overrides["cache_namespace"] = cache_namespace
        return self

    def with_dns_cache(self, dns_cache_ttl: str = "0s") -> Self:
        self._overrides["dns_cache_ttl"] = dns_cache_ttl
        return self

    def with_failure_policy(
        self, cache_failure_policy: Literal["fail-open", "fail-closed"] | str
    ) -> Self:
        self._overrides["cache_failure_policy"] = cache_failure_policy
        return self

    def with_probe_interval(self, probe_interval: float) -> Self:
        self._overrides["probe_interval"] = probe_interval
        return self

    def with_startup_timeout(self, startup_timeout: float) -> Self:
        self._overrides["startup_timeout"] = startup_timeout
        return self

    def with_circuit_breaker(
        self,
        circuit_breaker_enabled: bool | None = None,
        *,
        circuit_breaker_errors_rate: int | None = None,
        circuit_breaker_period: str | None = None,
        circuit_breaker_ttl: str | None = None,
        circuit_breaker_min_calls: int | None = None,
        circuit_breaker_max_destinations: int | None = None,
    ) -> Self:
        """Configure circuit breaker options."""
        if circuit_breaker_enabled is None and any(
            x is not None
            for x in (
                circuit_breaker_errors_rate,
                circuit_breaker_period,
                circuit_breaker_ttl,
                circuit_breaker_min_calls,
                circuit_breaker_max_destinations,
            )
        ):
            circuit_breaker_enabled = True

        if circuit_breaker_enabled is not None:
            self._overrides["circuit_breaker_enabled"] = circuit_breaker_enabled
        if circuit_breaker_errors_rate is not None:
            self._overrides["circuit_breaker_errors_rate"] = circuit_breaker_errors_rate
        if circuit_breaker_period is not None:
            self._overrides["circuit_breaker_period"] = circuit_breaker_period
        if circuit_breaker_ttl is not None:
            self._overrides["circuit_breaker_ttl"] = circuit_breaker_ttl
        if circuit_breaker_min_calls is not None:
            self._overrides["circuit_breaker_min_calls"] = circuit_breaker_min_calls
        if circuit_breaker_max_destinations is not None:
            self._overrides["circuit_breaker_max_destinations"] = (
                circuit_breaker_max_destinations
            )
        return self

    def with_circuit_breaker_disabled(self) -> Self:
        self._overrides["circuit_breaker_enabled"] = False
        return self

    def with_rate_limit(
        self,
        rate_limit: int | None = None,
        *,
        rate_limit_enabled: bool | None = None,
        rate_limit_period: str | None = None,
        rate_limit_ttl: str | None = None,
    ) -> Self:
        """Configure client rate limiting options."""
        if rate_limit_enabled is None and any(
            x is not None for x in (rate_limit, rate_limit_period, rate_limit_ttl)
        ):
            rate_limit_enabled = True

        if rate_limit_enabled is not None:
            self._overrides["rate_limit_enabled"] = rate_limit_enabled
        if rate_limit is not None:
            self._overrides["rate_limit"] = rate_limit
        if rate_limit_period is not None:
            self._overrides["rate_limit_period"] = rate_limit_period
        if rate_limit_ttl is not None:
            self._overrides["rate_limit_ttl"] = rate_limit_ttl
        return self

    def with_rate_limit_disabled(self) -> Self:
        self._overrides["rate_limit_enabled"] = False
        return self

    def build(self) -> ProxyCache:
        """Construct the configured ProxyCache instance."""
        from ..proxy_cache import ProxyCache

        return ProxyCache(settings=self.build_settings())

    def build_proxy(self, port: int | None = None) -> ProxyBuilder:
        """Create a ProxyBuilder seeded with this cache builder's configuration."""
        from ..proxy import ProxyBuilder

        builder = ProxyBuilder(settings=self.build_settings()).with_cache(self.build())
        if port is not None:
            builder.with_port(port)
        return builder

    async def __aenter__(self) -> ProxyCache:
        self._built_cache = self.build()
        return await self._built_cache.__aenter__()

    async def __aexit__(self, *args: object) -> None:
        if self._built_cache is not None:
            await self._built_cache.__aexit__(*args)

    def __enter__(self) -> ProxyCache:
        self._built_cache = self.build()
        return self._built_cache.__enter__()

    def __exit__(self, *args: object) -> None:
        if self._built_cache is not None:
            self._built_cache.__exit__(*args)


__all__ = [
    "ProxyCacheBuilder",
    "SettingsBuilderMixin",
]
