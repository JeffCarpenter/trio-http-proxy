"""Fluent builder API for ProxyCache and Proxy configuration."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Self

from .settings import Settings

if TYPE_CHECKING:
    from ..proxy import ProxyBuilder
    from ..proxy_cache import ProxyCache


def _normalize_cache_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    norm = dict(kwargs)
    if "enabled" in norm and "cache_enabled" not in norm:
        norm["cache_enabled"] = norm.pop("enabled")
    if "namespace" in norm and "cache_namespace" not in norm:
        norm["cache_namespace"] = norm.pop("namespace")
    if "failure_policy" in norm and "cache_failure_policy" not in norm:
        norm["cache_failure_policy"] = norm.pop("failure_policy")
    return norm


class ProxyCacheBuilder:
    """Fluent builder for constructing and configuring ProxyCache instances.

    Encapsulates cache backend, circuit breaker, and rate limiting configuration
    on top of the consolidated Settings data model.
    """

    def __init__(self, settings: Settings | None = None, **kwargs: Any) -> None:
        self._base_settings: Settings | None = settings
        self._overrides: dict[str, Any] = _normalize_cache_kwargs(kwargs)
        self._built_cache: ProxyCache | None = None

    def with_cache(
        self,
        url: str | None = None,
        *,
        enabled: bool | None = None,
        namespace: str | None = None,
        dns_cache_ttl: str | None = None,
        failure_policy: Literal["fail-open", "fail-closed"] | str | None = None,
        probe_interval: float | None = None,
        startup_timeout: float | None = None,
        cache_url: str | None = None,
        cache_enabled: bool | None = None,
        cache_namespace: str | None = None,
        cache_failure_policy: Literal["fail-open", "fail-closed"] | str | None = None,
    ) -> Self:
        """Configure cache backend and lifecycle options."""
        if url is None and cache_url is not None:
            url = cache_url
        if enabled is None and cache_enabled is not None:
            enabled = cache_enabled
        if namespace is None and cache_namespace is not None:
            namespace = cache_namespace
        if failure_policy is None and cache_failure_policy is not None:
            failure_policy = cache_failure_policy

        if url is not None:
            self._overrides["cache_url"] = url
        if enabled is not None:
            self._overrides["cache_enabled"] = enabled
        if namespace is not None:
            self._overrides["cache_namespace"] = namespace
        if dns_cache_ttl is not None:
            self._overrides["dns_cache_ttl"] = dns_cache_ttl
        if failure_policy is not None:
            self._overrides["cache_failure_policy"] = failure_policy
        if probe_interval is not None:
            self._overrides["probe_interval"] = probe_interval
        if startup_timeout is not None:
            self._overrides["startup_timeout"] = startup_timeout
        return self

    def with_cache_url(self, url: str) -> Self:
        self._overrides["cache_url"] = url
        return self

    def with_cache_enabled(self, enabled: bool) -> Self:
        self._overrides["cache_enabled"] = enabled
        return self

    def with_namespace(self, namespace: str) -> Self:
        self._overrides["cache_namespace"] = namespace
        return self

    def with_dns_cache(self, ttl: str = "0s") -> Self:
        self._overrides["dns_cache_ttl"] = ttl
        return self

    def with_failure_policy(
        self, policy: Literal["fail-open", "fail-closed"] | str
    ) -> Self:
        self._overrides["cache_failure_policy"] = policy
        return self

    def with_probe_interval(self, seconds: float) -> Self:
        self._overrides["probe_interval"] = seconds
        return self

    def with_startup_timeout(self, seconds: float) -> Self:
        self._overrides["startup_timeout"] = seconds
        return self

    def with_circuit_breaker(
        self,
        enabled: bool | None = None,
        *,
        errors_rate: int | None = None,
        period: str | None = None,
        ttl: str | None = None,
        min_calls: int | None = None,
        max_destinations: int | None = None,
        circuit_breaker_enabled: bool | None = None,
        circuit_breaker_errors_rate: int | None = None,
        circuit_breaker_period: str | None = None,
        circuit_breaker_ttl: str | None = None,
        circuit_breaker_min_calls: int | None = None,
        circuit_breaker_max_destinations: int | None = None,
    ) -> Self:
        """Configure circuit breaker options."""
        if enabled is None and circuit_breaker_enabled is not None:
            enabled = circuit_breaker_enabled
        if errors_rate is None and circuit_breaker_errors_rate is not None:
            errors_rate = circuit_breaker_errors_rate
        if period is None and circuit_breaker_period is not None:
            period = circuit_breaker_period
        if ttl is None and circuit_breaker_ttl is not None:
            ttl = circuit_breaker_ttl
        if min_calls is None and circuit_breaker_min_calls is not None:
            min_calls = circuit_breaker_min_calls
        if max_destinations is None and circuit_breaker_max_destinations is not None:
            max_destinations = circuit_breaker_max_destinations

        if enabled is None and any(
            x is not None
            for x in (errors_rate, period, ttl, min_calls, max_destinations)
        ):
            enabled = True

        if enabled is not None:
            self._overrides["circuit_breaker_enabled"] = enabled
        if errors_rate is not None:
            self._overrides["circuit_breaker_errors_rate"] = errors_rate
        if period is not None:
            self._overrides["circuit_breaker_period"] = period
        if ttl is not None:
            self._overrides["circuit_breaker_ttl"] = ttl
        if min_calls is not None:
            self._overrides["circuit_breaker_min_calls"] = min_calls
        if max_destinations is not None:
            self._overrides["circuit_breaker_max_destinations"] = max_destinations
        return self

    def with_circuit_breaker_disabled(self) -> Self:
        self._overrides["circuit_breaker_enabled"] = False
        return self

    def with_rate_limit(
        self,
        rate_limit: int | None = None,
        *,
        enabled: bool | None = None,
        limit: int | None = None,
        period: str | None = None,
        ttl: str | None = None,
        rate_limit_enabled: bool | None = None,
        rate_limit_period: str | None = None,
        rate_limit_ttl: str | None = None,
    ) -> Self:
        """Configure client rate limiting options."""
        if rate_limit is None and limit is not None:
            rate_limit = limit
        if enabled is None and rate_limit_enabled is not None:
            enabled = rate_limit_enabled
        if period is None and rate_limit_period is not None:
            period = rate_limit_period
        if ttl is None and rate_limit_ttl is not None:
            ttl = rate_limit_ttl

        if enabled is None and any(x is not None for x in (rate_limit, period, ttl)):
            enabled = True

        if enabled is not None:
            self._overrides["rate_limit_enabled"] = enabled
        if rate_limit is not None:
            self._overrides["rate_limit"] = rate_limit
        if period is not None:
            self._overrides["rate_limit_period"] = period
        if ttl is not None:
            self._overrides["rate_limit_ttl"] = ttl
        return self

    def with_rate_limit_disabled(self) -> Self:
        self._overrides["rate_limit_enabled"] = False
        return self

    def with_settings(self, settings: Settings) -> Self:
        self._base_settings = settings
        return self

    def build_settings(self) -> Settings:
        """Produce the unified Settings instance reflecting builder configuration."""
        if self._base_settings is not None:
            data = {
                k: getattr(self._base_settings, k)
                for k in self._base_settings.model_fields_set
            }
            data.update(self._overrides)
            return Settings(**data)
        return Settings(**self._overrides)

    def build(self) -> ProxyCache:
        """Construct the configured ProxyCache instance."""
        from ..proxy_cache import ProxyCache

        return ProxyCache(builder=self)

    def build_proxy(self, port: int | None = None) -> ProxyBuilder:
        """Create a ProxyBuilder seeded with this cache builder's configuration."""
        from ..proxy import ProxyBuilder

        builder = ProxyBuilder(settings=self.build_settings()).with_cache(self)
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
]
