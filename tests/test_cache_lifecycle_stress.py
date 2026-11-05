"""Event-gated batches exercise real backend work across the Trio bridge."""

import asyncio
from concurrent.futures import CancelledError

import pytest
import trio
from cache_fakes import ControlledBackend

from trio_http_proxy import ProxyCache
from trio_http_proxy.errors import CacheBackendUnavailable


class PendingReads:
    """A backend effect whose reads can fail together or be cancelled in flight."""

    def __init__(self, count):
        self.count = count
        self.token = trio.lowlevel.current_trio_token()
        self.all_started = trio.Event()
        self.all_finished = trio.Event()
        self.reads = []
        self.finished = []
        self.cancelled = []

    async def get(self, key, default=None):
        gate = asyncio.get_running_loop().create_future()
        self.reads.append((key, gate))
        if len(self.reads) == self.count:
            self.token.run_sync_soon(self.all_started.set)
        try:
            return await gate
        finally:
            self.finished.append(key)
            if gate.cancelled():
                self.cancelled.append(key)
            if len(self.finished) == self.count:
                self.token.run_sync_soon(self.all_finished.set)

    def fail_all(self):
        for _, gate in self.reads:
            gate.get_loop().call_soon_threadsafe(
                gate.set_exception, ConnectionError("backend disconnected")
            )


async def other_bridge_tasks():
    current = asyncio.current_task()
    return [task for task in asyncio.all_tasks() if task is not current]


@pytest.mark.parametrize("cycle", range(3))
async def test_batch_cancellation_drains_bridge_and_preserves_cache(cycle):
    backend = ControlledBackend()
    cache = ProxyCache(backend=backend, dns_cache_ttl="60s")
    async with cache:
        worker, loop = cache._thread, cache._loop
        assert worker is not None and loop is not None
        baseline_tasks = set(await cache.run(other_bridge_tasks))
        with trio.fail_after(15):
            # Several waves on the same bridge catch cancellation residue that
            # would be hidden by creating a fresh cache for every request.
            for wave in range(3):
                reads = PendingReads(16)
                backend.effects["get"] = reads.get
                async with trio.open_nursery() as nursery:
                    for index in range(reads.count):
                        nursery.start_soon(
                            cache.resolve_hosts, f"cancel-{cycle}-{wave}-{index}.test"
                        )
                    await reads.all_started.wait()
                    nursery.cancel_scope.cancel()
                # Future cancellation is asynchronous on the bridge thread.
                await reads.all_finished.wait()
                assert len(reads.cancelled) == reads.count
                assert set(await cache.run(other_bridge_tasks)) == baseline_tasks
                assert cache.is_running
                assert not cache.is_degraded

            backend.effects.pop("get")
            await cache.run(backend.set, "healthy", b"still usable")
            assert await cache.run(backend.get, "healthy") == b"still usable"
    assert cache.is_stopped
    assert not worker.is_alive()
    assert loop.is_closed()
    assert backend.operations.count("close") == 1
    assert all(task.done() for task in baseline_tasks)


@pytest.mark.parametrize("cycle", range(3))
async def test_concurrent_close_cancels_pending_backend_reads_once(cycle):
    backend = ControlledBackend()
    reads = PendingReads(16)
    backend.effects["get"] = reads.get
    close_snapshots = []

    async def close():
        close_snapshots.append((len(reads.finished), await other_bridge_tasks()))
        await backend.storage.close()

    backend.effects["close"] = close
    cache = ProxyCache(backend=backend, dns_cache_ttl="60s")
    outcomes = []
    async with cache:
        worker, loop = cache._thread, cache._loop
        assert worker is not None and loop is not None
        with trio.fail_after(15):

            async def read(index):
                try:
                    await cache.run(backend.get, f"shutdown-{cycle}-{index}")
                except CancelledError:
                    outcomes.append(index)
                else:
                    pytest.fail("Shutdown must cancel pending backend work")

            async with trio.open_nursery() as requests:
                for index in range(reads.count):
                    requests.start_soon(read, index)
                await reads.all_started.wait()
                async with trio.open_nursery() as closers:
                    for _ in range(4):
                        closers.start_soon(cache.aclose)
            await reads.all_finished.wait()
            assert sorted(outcomes) == list(range(reads.count))
            assert len(reads.cancelled) == reads.count
            assert close_snapshots == [(reads.count, [])]
            assert cache.is_stopped
            assert cache._thread is None
            assert not worker.is_alive()
            assert loop.is_closed()
            assert backend.operations.count("close") == 1

            # Rejected work must never construct a coroutine on the closed loop.
            constructed = []

            def rejected_work():
                constructed.append(True)
                return other_bridge_tasks()

            with pytest.raises(RuntimeError, match="stopped"):
                await cache.run(rejected_work)
            assert constructed == []
    # Context exit is another close caller and must remain idempotent.
    assert backend.operations.count("close") == 1


@pytest.mark.parametrize("policy", ["fail-open", "fail-closed"])
async def test_backend_outage_releases_concurrent_requests_and_recovers(policy):
    backend = ControlledBackend()
    reads = PendingReads(16)
    backend.effects["get"] = reads.get
    now = [1000.0]
    lookups = []

    def resolve(host):
        lookups.append(host)
        return ["127.0.0.1"]

    cache = ProxyCache(
        backend=backend,
        resolver=resolve,
        clock=lambda: now[0],
        dns_cache_ttl="60s",
        cache_failure_policy=policy,
        probe_interval=1.0,
    )
    hosts = [f"outage-{index}.test" for index in range(reads.count)]
    results, errors = {}, []
    async with cache:
        worker, loop = cache._thread, cache._loop
        assert worker is not None and loop is not None
        baseline_tasks = set(await cache.run(other_bridge_tasks))
        with trio.fail_after(15):

            async def resolve_during_outage(host):
                try:
                    results[host] = await cache.resolve_hosts(host)
                except CacheBackendUnavailable:
                    errors.append(host)

            async with trio.open_nursery() as requests:
                for host in hosts:
                    requests.start_soon(resolve_during_outage, host)
                await reads.all_started.wait()
                reads.fail_all()
            await reads.all_finished.wait()
            assert reads.cancelled == []
            assert lookups == []
            if policy == "fail-open":
                assert results == {host: [host] for host in hosts}
                assert errors == []
                assert cache.is_degraded
            else:
                assert results == {}
                assert sorted(errors) == sorted(hosts)
                assert not cache.is_degraded
            assert set(await cache.run(other_bridge_tasks)) == baseline_tasks

            backend.effects.pop("get")
            now[0] += 2.0
            pings = backend.operations.count("ping")
            assert await cache.resolve_hosts("recovered.test") == ["127.0.0.1"]
            assert await cache.resolve_hosts("recovered.test") == ["127.0.0.1"]
            assert lookups == ["recovered.test"]
            assert backend.operations.count("ping") == pings + (policy == "fail-open")
            assert cache.is_running
            assert not cache.is_degraded
    assert cache.is_stopped
    assert not worker.is_alive()
    assert loop.is_closed()
    assert backend.operations.count("close") == 1
    assert all(task.done() for task in baseline_tasks)
