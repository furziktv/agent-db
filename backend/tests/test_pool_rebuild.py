import asyncio

import pytest

from app.agent import manager as manager_module
from app.agent.manager import AgentManager


class FakePool:
    def __init__(self, name):
        self.name = name
        self.closed = False


@pytest.mark.asyncio
async def test_pool_is_rebuilt_once_for_concurrent_failed_workers(monkeypatch):
    manager = AgentManager(store=None)
    old_pool = FakePool("old")
    replacement = FakePool("new")
    manager.pool = old_pool
    manager.config.connection.retries = 0
    calls = 0

    async def create_pool(config, password):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0)
        return replacement

    async def close_pool(pool):
        pool.closed = True

    monkeypatch.setattr(manager_module, "create_pool", create_pool)
    monkeypatch.setattr(manager_module, "close_pool", close_pool)

    results = await asyncio.gather(
        manager.rebuild_pool(old_pool),
        manager.rebuild_pool(old_pool),
    )

    assert results == [True, True]
    assert calls == 1
    assert manager.pool is replacement
    assert old_pool.closed


@pytest.mark.asyncio
async def test_pool_rebuild_stops_cleanly_when_agent_stops(monkeypatch):
    manager = AgentManager(store=None)
    old_pool = FakePool("old")
    manager.pool = old_pool
    manager.stop_event.set()

    async def unexpected_create_pool(config, password):
        raise AssertionError("must not connect after stop")

    monkeypatch.setattr(manager_module, "create_pool", unexpected_create_pool)
    assert not await manager.rebuild_pool(old_pool)
    assert manager.pool is old_pool
