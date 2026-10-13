from app.database.pool import ConnectionLifecycle

class Connection:
    def __init__(self):
        self.closed = False
        self._reader = object()
        self.close_calls = 0
    def close(self):
        self.closed = True
        self._reader = None
        self.close_calls += 1

def test_connection_is_reused_four_times_then_discarded():
    lifecycle = ConnectionLifecycle()
    conn = Connection()
    for _ in range(4):
        assert not lifecycle.complete(conn)
        assert not conn.closed
    assert lifecycle.complete(conn)
    assert conn.closed and conn.close_calls == 1
    assert conn not in lifecycle.uses
    fresh = Connection()
    assert not lifecycle.complete(fresh)
    assert lifecycle.uses[fresh] == 1

def test_counts_are_per_connection_and_shared_across_workers():
    lifecycle = ConnectionLifecycle()
    first, second = Connection(), Connection()
    for _ in range(4):
        lifecycle.complete(first)
        lifecycle.complete(second)
    assert lifecycle.complete(first)
    assert not second.closed
    assert lifecycle.complete(second)

def test_closed_connections_do_not_accumulate_in_tracking():
    lifecycle = ConnectionLifecycle()
    old, fresh = Connection(), Connection()
    lifecycle.complete(old)
    old.close()
    lifecycle.complete(fresh)
    assert old not in lifecycle.uses
    lifecycle.forget(fresh)
    assert not lifecycle.uses

import asyncio
from unittest.mock import AsyncMock
import pytest
from app.database.pool import ManagedPool

@pytest.mark.asyncio
async def test_disconnected_release_discards_and_wakes_waiting_workers():
    conn = Connection()
    conn._connected = True
    raw = type('RawPool', (), {})()
    raw.acquire = AsyncMock(return_value=conn)
    raw.release = AsyncMock()
    raw._wakeup = AsyncMock()
    pool = ManagedPool(raw)
    async with pool.acquire() as borrowed:
        borrowed.close()
    assert conn._connected is False
    raw.release.assert_awaited_once_with(conn)
    raw._wakeup.assert_awaited_once()
