"""Lifecycle admission, shutdown ownership, and health are separate contracts."""

import asyncio
import queue
import threading

import pytest
import trio.testing as tt
from cache_fakes import ControlledBackend

from trio_http_proxy.cache import LifecycleState, ProxyCache


@pytest.mark.parametrize("join_failure", ["timeout", "exception"])
def test_shutdown_retry_reuses_inflight_cleanup(join_failure):
    backend = ControlledBackend()
    close_entered = threading.Event()
    close_cancelled = threading.Event()
    close_gate = []
    close_calls = []
    workers = []

    async def close():
        close_calls.append(1)
        gate = asyncio.get_running_loop().create_future()
        close_gate.append(gate)
        close_entered.set()
        try:
            await gate
        except asyncio.CancelledError:
            close_cancelled.set()
            raise
        await backend.storage.close()

    backend.effects["close"] = close

    class TimeoutOnceThread:
        def __init__(self, **kwargs):
            self.worker = threading.Thread(**kwargs)
            self.joins = 0
            workers.append(self)

        def start(self):
            self.worker.start()

        def is_alive(self):
            return self.worker.is_alive()

        def join(self, timeout=None):
            self.joins += 1
            if self.joins == 1:
                # Model a join timeout while backend.close is still pending.
                assert close_entered.wait(2)
                if join_failure == "exception":
                    raise RuntimeError("injected join failure")
                return
            gate = close_gate[0]
            gate.get_loop().call_soon_threadsafe(gate.set_result, None)
            self.worker.join(timeout)

    cache = ProxyCache(
        backend=backend, thread_factory=TimeoutOnceThread, rate_limit_enabled=True
    )
    cache.start()
    try:
        with pytest.raises(
            RuntimeError, match="Timed out stopping|injected join failure"
        ):
            cache.stop()
        assert cache.lifecycle_state is LifecycleState.STOPPING
        assert not cache.is_stopped
        with pytest.raises(RuntimeError, match="cannot be restarted"):
            cache.start()
        rejected = asyncio.sleep(0)
        with pytest.raises(RuntimeError, match="stopped"):
            cache.run_coroutine(rejected)
        assert rejected.cr_frame is None
        cache.stop()
        assert cache.lifecycle_state is LifecycleState.STOPPED
        assert not workers[0].is_alive()
        assert close_calls == [1]
        assert not close_cancelled.is_set()
    finally:
        # A failing assertion must not leave a test bridge thread alive.
        if workers[0].is_alive():
            gate = close_gate[0]
            if not gate.done():
                gate.get_loop().call_soon_threadsafe(gate.set_result, None)
            workers[0].worker.join(2)


def test_startup_timeout_stops_loop_published_at_deadline(monkeypatch):
    running = threading.Event()
    published_loop = []
    real_queue = queue.Queue

    class TimeoutAtPublication(real_queue):
        def put(self, item, *args, **kwargs):
            super().put(item, *args, **kwargs)
            loop, error = item
            assert error is None
            published_loop.append(loop)
            loop.call_soon(running.set)

        def get(self, block=True, timeout=None):
            if not block:
                return super().get(block=False)
            assert running.wait(2)
            raise queue.Empty

    monkeypatch.setattr("trio_http_proxy.proxy_cache.queue.Queue", TimeoutAtPublication)
    cache = ProxyCache(backend=ControlledBackend(), rate_limit_enabled=True)
    with pytest.raises(RuntimeError, match="Timed out waiting"):
        cache.start()
    worker = cache._thread
    assert worker is not None
    try:
        worker.join(1)
        assert not worker.is_alive(), "late-published loop survived startup abort"
        assert cache.lifecycle_state is LifecycleState.STOPPED
    finally:
        # The injected queue retains the loop for deterministic failure cleanup.
        if worker.is_alive():
            loop = published_loop[0] if published_loop else None
            if loop is not None:
                loop.call_soon_threadsafe(loop.stop)
            worker.join(2)


def test_startup_ping_timeout_cancels_abandoned_ping():
    backend = ControlledBackend()
    ping_finished = threading.Event()

    async def ping():
        try:
            await asyncio.get_running_loop().create_future()
        finally:
            ping_finished.set()

    backend.effects["ping"] = ping
    cache = ProxyCache(
        backend=backend,
        rate_limit_enabled=True,
        cache_failure_policy="fail-open",
        startup_timeout=0.02,
    )
    try:
        cache.start()
        assert cache.lifecycle_state is LifecycleState.RUNNING
        assert cache.is_degraded
        assert not cache.is_running
        assert ping_finished.wait(1), "startup timeout left a backend ping alive"
    finally:
        cache.stop()


def test_stop_during_startup_cannot_resurrect_cache():
    backend = ControlledBackend()
    ping_entered = threading.Event()
    errors = []

    async def ping():
        ping_entered.set()
        await asyncio.get_running_loop().create_future()

    backend.effects["ping"] = ping
    cache = ProxyCache(backend=backend, rate_limit_enabled=True, startup_timeout=2)

    def start():
        try:
            cache.start()
        except BaseException as exc:  # noqa: BLE001 -- report worker failures on the main thread
            errors.append(exc)

    starter = threading.Thread(target=start)
    starter.start()
    try:
        assert ping_entered.wait(2)
        cache.stop()
        starter.join(2)
        assert not starter.is_alive()
        assert errors == []
        assert cache.lifecycle_state is LifecycleState.STOPPED
        assert not cache.is_running
        assert cache._loop is None
        assert cache._thread is None
    finally:
        cache.stop()
        starter.join(2)


@tt.trio_test
async def test_async_submission_does_not_construct_rejected_coroutine():
    cache = ProxyCache(enabled=False)
    cache.stop()
    calls = []

    async def work():
        pass

    def factory():
        calls.append(1)
        return work()

    with pytest.raises(RuntimeError, match="stopped"):
        await cache.run(factory)
    assert calls == []


def test_concurrent_stop_callers_share_cleanup():
    backend = ControlledBackend()
    barrier = threading.Barrier(5)
    close_calls = []
    errors = []

    async def close():
        close_calls.append(1)
        await backend.storage.close()

    backend.effects["close"] = close
    cache = ProxyCache(backend=backend, rate_limit_enabled=True)
    cache.start()
    worker = cache._thread

    def stop():
        try:
            barrier.wait(timeout=2)
            cache.stop()
        except BaseException as exc:  # noqa: BLE001 -- report worker failures on the main thread
            errors.append(exc)

    callers = [threading.Thread(target=stop) for _ in range(4)]
    for caller in callers:
        caller.start()
    try:
        barrier.wait(timeout=2)
        for caller in callers:
            caller.join(3)
        assert all(not caller.is_alive() for caller in callers)
        assert errors == []
        assert close_calls == [1]
        assert cache.lifecycle_state is LifecycleState.STOPPED
        assert worker is not None and not worker.is_alive()
    finally:
        cache.stop()
        for caller in callers:
            caller.join(3)
