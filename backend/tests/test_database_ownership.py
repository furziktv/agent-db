from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
import pytest
from app.database.manager import DatabaseManager

RECORD = {
    'host': '10.100.0.5', 'port': 3306,
    'name': 'workload_test_regression',
    'server_uuid': 'original-server', 'owner_token': 'registered-marker',
}

class Cursor:
    def __init__(self, server_uuid, marker):
        self.results = iter([(server_uuid,), (marker,) if marker else None])
        self.execute = AsyncMock()
    async def __aenter__(self): return self
    async def __aexit__(self, *args): return None
    async def fetchone(self): return next(self.results)

class Connection:
    def __init__(self, server_uuid, marker): self.cur = Cursor(server_uuid, marker)
    def cursor(self): return self.cur

@pytest.fixture
def manager():
    agent = SimpleNamespace(
        config=SimpleNamespace(connection=SimpleNamespace(host='10.100.0.5', port=3306)),
        system=Mock(),
    )
    return DatabaseManager(agent, None)

@pytest.mark.asyncio
async def test_start_accepts_replicated_owned_database_after_primary_change(manager):
    conn = Connection('new-primary', 'registered-marker')
    await manager.owned(RECORD, conn, allow_server_change=True)
    manager.agent.system.assert_called_once()
    assert RECORD['server_uuid'] == 'original-server'
    assert conn.cur.execute.call_args.args[1] == ('registered-marker',)

@pytest.mark.asyncio
@pytest.mark.parametrize('relaxed', [False, True])
@pytest.mark.parametrize('marker', [None, 'different-marker'])
async def test_missing_or_foreign_marker_is_rejected_even_on_original_server(manager, relaxed, marker):
    with pytest.raises(ValueError, match='Ownership marker mismatch'):
        await manager.owned(RECORD, Connection('original-server', marker), allow_server_change=relaxed)

@pytest.mark.asyncio
async def test_strict_identity_check_rejects_changed_server(manager):
    with pytest.raises(ValueError, match='destructive actions require the original server'):
        await manager.owned(RECORD, Connection('new-primary', 'registered-marker'))

@pytest.mark.asyncio
async def test_original_server_and_marker_are_accepted(manager):
    await manager.owned(RECORD, Connection('original-server', 'registered-marker'))
    manager.agent.system.assert_not_called()

@pytest.mark.asyncio
async def test_changed_endpoint_is_not_implicitly_trusted(manager):
    record = dict(RECORD, host='other.example')
    with pytest.raises(ValueError, match='registered server'):
        await manager.owned(record, Connection('new-primary', 'registered-marker'), allow_server_change=True)

@pytest.mark.asyncio
@pytest.mark.parametrize('action', ['delete', 'clear'])
@pytest.mark.parametrize('read_only,super_read_only', [(0, 0), (1, 0), (0, 1)])
async def test_management_after_failover_requires_owned_writable_connection(manager, monkeypatch, action, read_only, super_read_only):
    conn = Connection('new-primary', 'registered-marker')
    conn.cur.results = iter([('new-primary',), ('registered-marker',), (read_only, super_read_only)])
    conn.close = Mock()
    conn.begin = AsyncMock()
    conn.commit = AsyncMock()
    conn.rollback = AsyncMock()
    connect = AsyncMock(return_value=conn)
    monkeypatch.setattr('app.database.manager.connect', connect)
    manager.agent.state = 'STOPPED'
    manager.agent.password = 'synthetic-test-secret'
    manager.store = SimpleNamespace(database=AsyncMock(return_value=dict(RECORD, status='READY')), save_database=AsyncMock())
    if read_only or super_read_only:
        with pytest.raises(ValueError, match='read-only'):
            await manager.action('registered-id', action, RECORD['name'])
        manager.store.save_database.assert_not_awaited()
    else:
        result = await manager.action('registered-id', action, RECORD['name'])
        assert result['status'] == ('DELETED' if action == 'delete' else 'READY')
        manager.store.save_database.assert_awaited_once()
    statements = [call.args[0] for call in conn.cur.execute.call_args_list]
    destructive = [sql for sql in statements if sql.startswith(('DROP ', 'DELETE ', 'UPDATE '))]
    assert bool(destructive) == (not read_only and not super_read_only)
    if action == 'delete' and destructive:
        assert destructive == ['DROP DATABASE `workload_test_regression`']
    conn.close.assert_called_once()

@pytest.mark.asyncio
@pytest.mark.parametrize('marker', [None, 'foreign-marker'])
async def test_delete_never_runs_for_unowned_database(manager, monkeypatch, marker):
    conn = Connection('new-primary', marker)
    conn.close = Mock()
    monkeypatch.setattr('app.database.manager.connect', AsyncMock(return_value=conn))
    manager.agent.state = 'STOPPED'
    manager.agent.password = 'synthetic-test-secret'
    manager.store = SimpleNamespace(database=AsyncMock(return_value=dict(RECORD)), save_database=AsyncMock())
    with pytest.raises(ValueError, match='Ownership marker mismatch'):
        await manager.action('registered-id', 'delete', RECORD['name'])
    assert not any(call.args[0].startswith('DROP ') for call in conn.cur.execute.call_args_list)
    manager.store.save_database.assert_not_awaited()
    conn.close.assert_called_once()

@pytest.mark.asyncio
async def test_delete_requires_exact_confirmation_before_connecting(manager, monkeypatch):
    connect = AsyncMock()
    monkeypatch.setattr('app.database.manager.connect', connect)
    manager.agent.state = 'STOPPED'
    manager.store = SimpleNamespace(database=AsyncMock(return_value=dict(RECORD)))
    with pytest.raises(ValueError, match='exact database name'):
        await manager.action('registered-id', 'delete', 'other_database')
    connect.assert_not_awaited()
