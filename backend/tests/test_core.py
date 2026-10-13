import asyncio
import json
import time
import pytest
from pydantic import ValidationError
from app.models.config import Config, ConnectionConfig, WorkloadConfig, DatabaseRequest, quote, LIMITS
from app.agent.rate_limiter import RateLimiter
from app.monitoring.collector import Collector, percentiles, utc
from app.agent.templates import TEMPLATES
from app.database.schema import NAMES, TABLES
from app.reports.manager import Store

@pytest.mark.parametrize('name',['x; DROP DATABASE y','x`','x.y','../mysql','mysql agent',''])
def test_identifier_rejects_injection(name):
    with pytest.raises(ValueError): quote(name)

@pytest.mark.parametrize('values',[{'select_percent':99},{'min_payload':100,'max_payload':10},
 {'workers':0},{'rps':0},{'pool_min_size':20,'pool_max_size':10},{'join_percent':21},
 {'workers':LIMITS['MAX_WORKERS']+1},{'max_payload':LIMITS['MAX_PAYLOAD_BYTES']+1}])
def test_invalid_workload(values):
    with pytest.raises(ValidationError): WorkloadConfig(**values)

def test_password_not_serialized():
    c=Config(connection=ConnectionConfig(password='SUPERSECRET'))
    assert 'SUPERSECRET' not in c.model_dump_json()
    assert 'password' not in c.model_dump()['connection']

def test_generator_confirmation_and_namespace():
    with pytest.raises(ValidationError): DatabaseRequest(name='production',confirmation='production')
    with pytest.raises(ValidationError): DatabaseRequest(name='workload_test_x',confirmation='wrong')

def test_real_relational_templates_and_safe_writes():
    assert len(TEMPLATES)>=30 and len(NAMES)>=12
    for table in NAMES:
        assert any(table in t.tables for t in TEMPLATES if t.kind=='simple')
        assert any(t.tables==(table,) for t in TEMPLATES if t.kind=='insert')
    assert sum(len(t.refs) for t in TABLES)>=15
    assert any('LEFT JOIN' in t.sql for t in TEMPLATES)
    assert any('INNER JOIN' in t.sql for t in TEMPLATES)
    assert any('SUM(' in t.sql for t in TEMPLATES)
    assert not any('ORDER BY RAND' in t.sql for t in TEMPLATES)

@pytest.mark.asyncio
async def test_global_rate_is_independent_of_workers():
    limiter=RateLimiter(100)
    start=time.monotonic()
    await asyncio.gather(*(limiter.acquire() for _ in range(51)))
    duration=time.monotonic()-start
    assert .48 <= duration <= 1.5

@pytest.mark.asyncio
async def test_low_rate_stop_is_prompt():
    limiter=RateLimiter(.00001)
    await limiter.acquire()
    stop=asyncio.Event()
    task=asyncio.create_task(limiter.acquire(stop))
    await asyncio.sleep(.01)
    stop.set()
    await asyncio.wait_for(task,.1)

def test_metrics():
    c=Collector()
    for latency in [1,2,3,4,5]:
        c.record(dict(timestamp=utc(),worker_id='worker',query_type='SELECT',query_class='simple',
          success=True,execution_time_ms=latency,tables=['users'],template='simple_users',rows_read=1))
    m=c.snapshot(['users','orders'])
    assert m['total']==5 and m['success']==5 and m['rows_read']==5
    assert m['p50']==3 and m['p95']==4.8
    assert m['table_coverage']=={'used':1,'total':2}

@pytest.mark.asyncio
async def test_sqlite_persistence_and_backpressure(tmp_path,monkeypatch):
    monkeypatch.setenv('SQLITE_PATH',str(tmp_path/'db.sqlite3'))
    store=Store()
    await store.open()
    await store.save_config(Config(connection=ConnectionConfig(password='SUPERSECRET')))
    for i in range(2001): await store.request('session',{'timestamp':utc(),'number':i})
    await store.queue.join()
    assert (await store.fetch('SELECT COUNT(*) FROM requests'))[0][0]==2001
    assert 'SUPERSECRET' not in json.dumps(await store.get_config())
    await store.close()
