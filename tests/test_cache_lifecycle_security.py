"""Tests for ProxyCache lifecycle state machine, thread cleanup, and security hardening.

Covers Tranche 4 requirements:
- §4.2: LifecycleState enum (NEW, STARTING, RUNNING, STOPPING, STOPPED), atomic transitions, restart prohibition
- §4.3: Shared abort event preventing orphan bridge threads on startup timeout
- §5.1: Credential redaction in URLs for logs and exceptions
- §5.2: Strict namespace regex validation (^[a-zA-Z0-9_-]+$)
- §5.3: Semantic duration parsing (rejecting "0m", "00s", non-positive TTLs)
- §5.4: Comprehensive bridge shutdown protocol (shutdown_asyncgens, executor cleanup)
"""

import logging
import threading

import pytest

from trio_http_proxy.cache import LifecycleState, ProxyCache
from trio_http_proxy.errors import CacheBackendUnavailable
from trio_http_proxy.util import redact_url


class TestNamespaceValidation:
    """Invariant §5.2: Strict namespace regex validation (^[a-zA-Z0-9_-]+$)."""

    @pytest.mark.parametrize(
        "invalid_namespace",
        [
            "cache:foo",
            "ns*glob",
            "cache name",
            "ns?key",
            "ns[1]",
            "ns/bar",
            "ns@key",
            "",
            "   ",
        ],
    )
    def test_namespace_validation_rejects_disallowed_characters(
        self, invalid_namespace: str
    ) -> None:
        with pytest.raises(ValueError, match="namespace"):
            ProxyCache(namespace=invalid_namespace)

    @pytest.mark.parametrize(
        "valid_namespace",
        [
            "default",
            "cache_123",
            "my-ns",
            "PROD-cache_01",
        ],
    )
    def test_namespace_validation_accepts_valid_identifier(
        self, valid_namespace: str
    ) -> None:
        cache = ProxyCache(namespace=valid_namespace)
        assert cache.namespace == valid_namespace


class TestSemanticDurationParsing:
    """Invariant §5.3 & §2.2: Semantic duration parsing and positive duration enforcement."""

    @pytest.mark.parametrize(
        "invalid_period",
        ["0m", "00s", "0", "-5s", "0h", "-1m"],
    )
    def test_rate_limit_period_rejects_non_positive_durations(
        self, invalid_period: str
    ) -> None:
        with pytest.raises(ValueError, match="rate_limit_period"):
            ProxyCache(rate_limit_period=invalid_period)

    @pytest.mark.parametrize(
        "zero_dns_ttl",
        ["0m", "00s", "0", "0s", "0h"],
    )
    def test_dns_cache_ttl_semantic_parsing_disables_caching(
        self, zero_dns_ttl: str
    ) -> None:
        cache = ProxyCache(dns_cache_ttl=zero_dns_ttl)
        assert cache.dns_cache_ttl_sec == 0.0


class TestUrlCredentialRedaction:
    """Invariant §5.1: Credential redaction in URLs for logs and exceptions."""

    def test_redact_url_function_exported(self) -> None:
        assert redact_url is not None, (
            "redact_url function must be exported from trio_http_proxy.util"
        )

    @pytest.mark.parametrize(
        ("raw_url", "expected_redacted"),
        [
            (
                "redis://user:secret123@localhost:6379/0",
                "redis://user:***@localhost:6379/0",
            ),
            (
                "redis://:my_password@redis.internal:6379",
                "redis://:***@redis.internal:6379",
            ),
            (
                "rediss://admin:p@ss:w0rd@cluster.cache.amazonaws.com:6379/1",
                "rediss://admin:***@cluster.cache.amazonaws.com:6379/1",
            ),
            (
                "redis://localhost:6379/0",
                "redis://localhost:6379/0",
            ),
            (
                "mem://",
                "mem://",
            ),
            (
                "",
                "",
            ),
        ],
    )
    def test_redact_url_sanitizes_userinfo(
        self, raw_url: str, expected_redacted: str
    ) -> None:
        assert redact_url is not None, (
            "redact_url function must be exported from trio_http_proxy.util"
        )
        assert redact_url(raw_url) == expected_redacted

    def test_cache_backend_unavailable_exception_redacts_credentials(self) -> None:
        raw_url = "redis://user:super_secret_pw@127.0.0.1:6379/0"
        cache = ProxyCache(
            cache_url=raw_url,
            cache_failure_policy="fail-closed",
            rate_limit_enabled=True,
            startup_timeout=0.2,
        )
        with pytest.raises(CacheBackendUnavailable) as exc_info:
            cache.start()

        msg = str(exc_info.value)
        assert "super_secret_pw" not in msg, (
            f"Password leaked in exception message: {msg}"
        )
        assert "user:***@127.0.0.1:6379" in msg

    def test_startup_failure_log_redacts_credentials(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        raw_url = "redis://app_user:secret_token_xyz@127.0.0.1:6379/0"
        cache = ProxyCache(
            cache_url=raw_url,
            cache_failure_policy="fail-open",
            rate_limit_enabled=True,
            startup_timeout=0.2,
        )
        with caplog.at_level(logging.WARNING):
            cache.start()
            cache.stop()

        for record in caplog.records:
            assert "secret_token_xyz" not in record.message, (
                f"Password leaked in log message: {record.message}"
            )


class TestLifecycleStateMachine:
    """Invariant §4.2: LifecycleState enum, atomic transitions & restart prohibition."""

    def test_lifecycle_state_enum_exported_with_required_members(self) -> None:
        assert LifecycleState is not None, (
            "LifecycleState enum must be exported from trio_http_proxy.cache"
        )
        member_names = [m.name for m in LifecycleState]
        expected_members = ["NEW", "STARTING", "RUNNING", "STOPPING", "STOPPED"]
        assert member_names == expected_members

    def test_initial_lifecycle_state_is_new(self) -> None:
        cache = ProxyCache(enabled=False)
        assert hasattr(cache, "lifecycle_state"), (
            "ProxyCache must expose lifecycle_state property"
        )
        assert cache.lifecycle_state == LifecycleState.NEW
        assert not cache.is_running
        assert not cache.is_stopped

    def test_lifecycle_state_transitions_to_running_and_stopped(self) -> None:
        cache = ProxyCache(enabled=False)
        cache.start()
        assert cache.lifecycle_state == LifecycleState.RUNNING
        assert cache.is_running
        assert not cache.is_stopped

        cache.stop()
        assert cache.lifecycle_state == LifecycleState.STOPPED
        assert not cache.is_running
        assert cache.is_stopped

    def test_restarting_stopped_cache_raises_runtime_error(self) -> None:
        cache = ProxyCache(enabled=False)
        cache.start()
        cache.stop()
        assert cache.lifecycle_state == LifecycleState.STOPPED

        with pytest.raises(
            RuntimeError, match="ProxyCache cannot be restarted once stopped"
        ):
            cache.start()

    def test_start_is_idempotent_when_already_running(self) -> None:
        cache = ProxyCache(enabled=False)
        cache.start()
        assert cache.lifecycle_state == LifecycleState.RUNNING

        # Calling start again while running remains RUNNING and does nothing
        cache.start()
        assert cache.lifecycle_state == LifecycleState.RUNNING

    def test_stop_is_idempotent_when_already_stopped(self) -> None:
        cache = ProxyCache(enabled=False)
        cache.start()
        cache.stop()
        assert cache.lifecycle_state == LifecycleState.STOPPED

        # Calling stop again while stopped remains STOPPED without error
        cache.stop()
        assert cache.lifecycle_state == LifecycleState.STOPPED


class TestStartupTimeoutThreadLeakPrevention:
    """Invariant §4.3: Startup timeout sets _startup_aborted preventing orphan threads."""

    def test_startup_aborted_event_initialized(self) -> None:
        cache = ProxyCache(enabled=False)
        assert hasattr(cache, "_startup_aborted"), (
            "ProxyCache must define _startup_aborted threading.Event"
        )
        assert isinstance(cache._startup_aborted, threading.Event)
        assert not cache._startup_aborted.is_set()

    def test_startup_timeout_aborts_delayed_bridge_thread(self) -> None:
        unblock_thread = threading.Event()
        thread_exited = threading.Event()

        def delayed_thread(*, target, daemon, name):
            def delayed_run() -> None:
                unblock_thread.wait(timeout=1.0)
                try:
                    target()
                finally:
                    thread_exited.set()

            return threading.Thread(target=delayed_run, daemon=daemon, name=name)

        cache = ProxyCache(
            cache_url="mem://",
            rate_limit_enabled=True,
            startup_timeout=0.05,
            thread_factory=delayed_thread,
        )
        try:
            with pytest.raises(RuntimeError, match="Timed out waiting"):
                cache.start()
            assert cache._startup_aborted.is_set()
            assert cache.lifecycle_state == LifecycleState.STOPPED
        finally:
            unblock_thread.set()
            assert thread_exited.wait(timeout=1.0)
            if cache._thread is not None:
                cache._thread.join(timeout=1.0)
                assert not cache._thread.is_alive()


class TestBridgeShutdownProtocol:
    """Invariant §5.4: Bridge thread shutdown runs shutdown_asyncgens and executor timeout."""

    def test_stop_invokes_shutdown_asyncgens(self) -> None:
        finalized = threading.Event()

        async def values():
            try:
                yield 1
                yield 2
            finally:
                finalized.set()

        with ProxyCache(cache_url="mem://", rate_limit_enabled=True) as cache:
            # Retain a reference so only loop shutdown, not GC, closes the generator.
            generator = values()

            async def first():
                return await anext(generator)

            assert cache.run_coroutine(first()) == 1
            assert not finalized.is_set()

        assert finalized.is_set(), "bridge shutdown must finalize live async generators"
        assert cache.lifecycle_state == LifecycleState.STOPPED
