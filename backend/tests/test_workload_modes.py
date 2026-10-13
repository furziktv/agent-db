import random
from types import SimpleNamespace
from unittest.mock import AsyncMock,Mock
import pytest
from pydantic import ValidationError
from app.models.config import Config,WorkloadConfig
from app.agent.worker import Worker
from app.agent.manager import AgentManager
from app.agent.integrity import PROBE_TABLE
from app.database.schema import NAMES
from app.agent.templates import TEMPLATES

@pytest.mark.parametrize('legacy,expected',[(True,'INTEGRITY'),(False,'NORMAL')])
def test_legacy_configs_keep_their_mode(legacy,expected):
    w=WorkloadConfig(integrity_test=legacy)
    assert w.mode==expected
    assert WorkloadConfig.model_validate(w.model_dump()).mode==expected

def test_explicit_mode_wins_over_legacy_boolean():
    w=WorkloadConfig(mode='MIXED',integrity_test=True)
    assert not w.integrity_test

@pytest.mark.parametrize('percentage',[-1,0,100,101])
def test_mixed_requires_both_kinds_of_workload(percentage):
    with pytest.raises(ValidationError): WorkloadConfig(mode='MIXED',integrity_percent=percentage)

@pytest.mark.parametrize('relational',[False,True])
def test_mixed_distribution_preserves_normal_weights_and_schema_coverage(relational):
    agent=AgentManager(SimpleNamespace())
    agent.config=Config(workload=WorkloadConfig(mode='MIXED',integrity_percent=30,relational=relational,complexity='LOW'))
    worker=Worker(agent,'test')
    worker.rng=random.Random(123)
    counts={}
    for _ in range(20000):
        op,kind,t=worker.choose()
        counts[kind]=counts.get(kind,0)+1
        if kind=='integrity': assert op=='INTEGRITY' and t is None
        elif relational: assert t and t in TEMPLATES
    assert abs(counts['integrity']/20000-.30)<.02
    normal=20000-counts['integrity']
    assert abs(counts['simple']/normal-(.25 if relational else .70))<.025
    assert PROBE_TABLE in agent.expected_tables()
    assert set(agent.expected_tables())==set((*NAMES,PROBE_TABLE) if relational else (agent.config.connection.table,PROBE_TABLE))

@pytest.mark.asyncio
async def test_mixed_normal_select_is_not_a_probe():
    cursor=AsyncMock()
    cursor.__aenter__.return_value=cursor
    cursor.fetchone.side_effect=[(1,),(1,)]
    cursor.fetchall.return_value=[(1,'test',4)]
    conn=Mock()
    conn.cursor.return_value=cursor
    manager=SimpleNamespace(config=Config(workload=WorkloadConfig(mode='MIXED')),limiter=SimpleNamespace(acquire=AsyncMock()))
    result=await Worker(manager,'test').execute(conn,'SELECT','simple',None,'',1)
    assert result['rows_read']==1
    assert 'integrity_checks' not in result
    assert all(PROBE_TABLE not in call.args[0] for call in cursor.execute.call_args_list)
    manager.limiter.acquire.assert_not_awaited()

@pytest.mark.asyncio
@pytest.mark.parametrize('relational',[False,True])
@pytest.mark.parametrize('auto_create',[False,True])
async def test_start_prepares_both_schemas_before_pool_or_workers(monkeypatch,relational,auto_create):
    store=SimpleNamespace(databases=AsyncMock(return_value=[{'name':'agent_test','status':'READY','host':'127.0.0.1','port':3306}]))
    agent=AgentManager(store)
    agent.config=Config(workload=WorkloadConfig(mode='MIXED',relational=relational,auto_create_table=auto_create))
    agent.config.connection.retries=0
    store.databases.return_value[0].update(name=agent.config.connection.database,host=agent.config.connection.host)
    agent.database_manager=SimpleNamespace(task=None,owned=AsyncMock())
    agent.verified=agent.fingerprint()
    cursor=AsyncMock()
    cursor.__aenter__.return_value=cursor
    cursor.fetchone.return_value=(1,)
    conn=Mock()
    conn.cursor.return_value=cursor
    monkeypatch.setattr('app.agent.manager.connect',AsyncMock(return_value=conn))
    # End START after schema validation, without creating actual workers or connections.
    pool=AsyncMock(side_effect=RuntimeError('synthetic pool failure'))
    monkeypatch.setattr('app.agent.manager.create_pool',pool)
    with pytest.raises(ValueError): await agent.start()
    pool.assert_awaited_once()
    statements=[call.args[0] for call in cursor.execute.call_args_list]
    assert any(f'FROM `{PROBE_TABLE}`' in sql for sql in statements)
    if relational:
        agent.database_manager.owned.assert_awaited_once()
        assert all(any(f'FROM `{table}`' in sql for sql in statements) for table in NAMES)
    else:
        assert any(f'FROM `{agent.config.connection.table}`' in sql for sql in statements)
    assert any(sql.startswith('CREATE TABLE') for sql in statements)==auto_create
    assert agent.state=='STOPPED' and not agent.workers
    conn.close.assert_called_once()
