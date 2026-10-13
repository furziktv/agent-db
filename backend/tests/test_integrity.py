import random
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from app.agent.integrity import probe, PROBE_TABLE
from app.agent.worker import Worker
from app.models.config import Config, WorkloadConfig
from app.monitoring.collector import Collector, utc

@pytest.mark.asyncio
@pytest.mark.parametrize('results,losses,missing,mismatches', [
    ([('', 1), ('', 2)], 0, 0, 0),
    ([None, ('', 2)], 1, 1, 0),
    ([('', 1), None], 1, 1, 0),
    ([None, None], 2, 2, 0),
    ([('wrong', 1), ('', 1)], 2, 0, 2),
])
async def test_committed_probe_detects_missing_payload_and_stale_revision(results, losses, missing, mismatches):
    run = AsyncMock(return_value=1)
    read = AsyncMock(side_effect=[[row] if row else [] for row in results])
    limiter = SimpleNamespace(acquire=AsyncMock())
    counters = dict(rows_inserted=0,rows_updated=0,bytes_generated=0)
    w = WorkloadConfig(integrity_test=True,min_payload=0,max_payload=0)
    await probe(run,read,counters,w,random.Random(1),limiter,'')
    assert counters['integrity_checks'] == 2
    assert counters['integrity_lost_writes'] == losses
    assert counters['integrity_missing'] == missing
    assert counters['integrity_mismatches'] == mismatches
    assert counters['rows_inserted'] == counters['rows_updated'] == 1
    assert 'payload' not in str(counters['integrity_details'])
    limiter.acquire.assert_awaited()
    assert limiter.acquire.await_count == 3  # INSERT takes its token in Worker.run.
    insert, update = run.call_args_list
    assert insert.args[0].startswith('INSERT INTO')
    assert update.args[0].endswith('WHERE probe_id=%s LIMIT 1')
    assert insert.args[1][0] == update.args[1][1] == read.call_args_list[0].args[1][0] == read.call_args_list[1].args[1][0]

@pytest.mark.asyncio
async def test_connection_error_does_not_assert_data_loss():
    run = AsyncMock(return_value=1)
    read = AsyncMock(side_effect=ConnectionError('test disconnect'))
    counters = dict(rows_inserted=0,rows_updated=0,bytes_generated=0)
    with pytest.raises(ConnectionError):
        await probe(run,read,counters,WorkloadConfig(),random.Random(),SimpleNamespace(acquire=AsyncMock()),'test')
    assert counters['integrity_checks'] == counters['integrity_lost_writes'] == 0
    assert run.await_count == 1

@pytest.mark.asyncio
async def test_workers_use_unique_probe_ids():
    run = AsyncMock(return_value=1)
    for _ in range(2):
        await probe(run,AsyncMock(return_value=[]),dict(rows_inserted=0,rows_updated=0,bytes_generated=0),WorkloadConfig(),random.Random(),SimpleNamespace(acquire=AsyncMock()),'test')
    assert run.call_args_list[0].args[1][0] != run.call_args_list[2].args[1][0]

def test_metrics_distinguish_verification_failures_and_inconclusive_checks():
    c = Collector()
    event = dict(timestamp=utc(), query_type='INTEGRITY', query_class='integrity',
        success=False,execution_time_ms=1,tables=[PROBE_TABLE],template='write_read_update_read',
        integrity_checks=1,integrity_lost_writes=1,integrity_missing=1,integrity_inconclusive=1,statements=2)
    c.record(event)
    m = c.snapshot([PROBE_TABLE])
    assert m['integrity_checks'] == m['integrity_lost_writes'] == m['integrity_inconclusive'] == 1
    assert m['integrity_loss_rps'] > 0 and m['sql_rps'] > 0
    assert m['failed'] == 1
    assert m.get('connection_errors',0) == 0

def test_integrity_mode_takes_priority_over_relational_distribution():
    manager = SimpleNamespace(config=Config(workload=WorkloadConfig(integrity_test=True,relational=True)))
    assert Worker(manager,'test-worker').choose() == ('INTEGRITY','integrity',None)

@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['none', 'missing', 'stale', 'disconnect'])
@pytest.mark.parametrize('mode', ['INTEGRITY','MIXED'])
async def test_worker_persists_probe_result_and_classifies_connection_failure(failure,mode):
    import asyncio
    from contextlib import asynccontextmanager
    from unittest.mock import Mock
    from app.monitoring.prometheus import Prometheus
    class Cursor:
        rowcount = 1
        def __init__(self):
            self.payload = ''
            self.revision = 0
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def execute(self, sql, args):
            if sql.startswith('INSERT'):
                self.payload, self.revision = args[1], 1
            elif sql.startswith('UPDATE'):
                self.payload, self.revision = args[0], 2
            elif failure == 'disconnect':
                raise ConnectionError('synthetic disconnect')
        async def fetchall(self):
            if failure == 'missing': return []
            return [(self.payload, 1 if failure == 'stale' else self.revision)]
    cursor = Cursor()
    conn = SimpleNamespace(cursor=lambda:cursor, close=Mock())
    @asynccontextmanager
    async def acquire(): yield conn
    stop = asyncio.Event()
    pause = asyncio.Event()
    pause.set()
    store = SimpleNamespace(request=AsyncMock(side_effect=lambda *args:stop.set()))
    manager = SimpleNamespace(config=Config(workload=WorkloadConfig(mode=mode,min_payload=0,max_payload=0)),
        pause_event=pause,stop_event=stop,limiter=SimpleNamespace(acquire=AsyncMock()),
        dispatched=0,pool=SimpleNamespace(acquire=acquire),active_connections=0,
        connection_lifecycle=SimpleNamespace(complete=Mock(return_value=False),forget=Mock()),
        collector=Collector(),prometheus=Prometheus(),store=store,session_id='synthetic-session',
        recovered=Mock(),disconnected=Mock(),rebuild_pool=AsyncMock())
    worker=Worker(manager,'worker-test')
    worker.choose=Mock(return_value=('INTEGRITY','integrity',None))
    await worker.run()
    event = store.request.call_args.args[1]
    if failure == 'disconnect':
        assert event['error_kind'] == 'connection_errors'
        assert event['sql_counts'] == {'SELECT':0,'INSERT':1,'UPDATE':0}
        assert event['integrity_inconclusive'] == 2
        assert event['integrity_lost_writes'] == 0
        manager.rebuild_pool.assert_awaited_once()
    else:
        assert event['statements'] == 4
        assert event['sql_counts'] == {'SELECT':2,'INSERT':1,'UPDATE':1}
        assert event['integrity_checks'] == 2
        assert event['integrity_lost_writes'] == {'none':0,'missing':2,'stale':1}[failure]
        assert event['success'] == (failure == 'none')
        assert manager.limiter.acquire.await_count == 4
    assert manager.active_connections == 0
    assert 'synthetic disconnect' not in str(event)

@pytest.mark.parametrize('counts,success', [({'SELECT':2,'INSERT':1,'UPDATE':1},True),({'SELECT':0,'INSERT':1,'UPDATE':0},False),({},False)])
def test_read_write_chart_counts_sql_inside_cycles(counts,success,monkeypatch):
    import app.monitoring.collector as module
    monkeypatch.setattr(module.time,'monotonic',lambda:100.0)
    collector=Collector()
    collector.started=99.0
    collector.record(dict(timestamp=utc(),query_type='INTEGRITY',query_class='integrity',
        success=success,execution_time_ms=1,tables=[PROBE_TABLE],template='write_read_update_read',sql_counts=counts))
    metrics=collector.snapshot()
    assert metrics['select_rps']==counts.get('SELECT',0)
    assert metrics['insert_rps']==counts.get('INSERT',0)
    assert metrics['update_rps']==counts.get('UPDATE',0)
    assert metrics['total']==metrics['INTEGRITY']==1
    assert metrics['sql_insert_total']==counts.get('INSERT',0)


def test_join_sql_is_counted_once_and_nested_transaction_activity_is_visible(monkeypatch):
    import app.monitoring.collector as module
    from app.monitoring.prometheus import Prometheus
    monkeypatch.setattr(module.time,'monotonic',lambda:100.0)
    collector=Collector()
    collector.started=99.0
    event=dict(timestamp=utc(),database_host='test.example',query_type='TRANSACTION',query_class='transaction',
        success=False,execution_time_ms=1,tables=['orders','users'],template='transaction_order',
        sql_counts={'SELECT':2,'INSERT':3,'UPDATE':1},table_stats={'orders':{'SELECT':2},'users':{'SELECT':2}})
    collector.record(event)
    metrics=collector.snapshot()
    assert metrics['select_rps']==2 and metrics['insert_rps']==3 and metrics['update_rps']==1
    assert metrics['transaction_rps']==1
    prometheus=Prometheus()
    prometheus.record(event)
    assert prometheus.registry.get_sample_value('mysql_agent_select_total',{'host':'test.example'})==2
