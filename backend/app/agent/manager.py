from app.agent.integrity import PROBE_TABLE, PROBE_DDL
import asyncio
import json
import logging
import time
import uuid
from app.models.config import Config, quote
from app.database.connection import connect, test_connection, safe_error
from app.database.pool import create_pool, close_pool, ConnectionLifecycle
from app.database.schema import NAMES
from app.agent.rate_limiter import RateLimiter
from app.agent.worker import Worker
from app.monitoring.collector import Collector, utc
from app.monitoring.prometheus import Prometheus

log=logging.getLogger('agent')

class AgentManager:
    def __init__(self,store):
        self.store=store
        self.process_started=time.monotonic()
        self.config=Config()
        self.password=''
        self.verified=None
        self.state='STOPPED'
        self.session_id=None
        self.dispatched=0
        self.started_at=None
        self.ended_at=None
        self.collector=Collector()
        self.prometheus=Prometheus()
        self.pool=None
        self.connection_lifecycle=ConnectionLifecycle()
        self.workers=[]
        self.stop_event=asyncio.Event()
        self.pause_event=asyncio.Event()
        self.pause_event.set()
        self.active_connections=0
        self.lock=asyncio.Lock()
        self.pool_rebuild_lock=asyncio.Lock()
        self.pool_rebuild_retry_after=0.0
        self.unavailable_from=None
        self.unavailable_mono=None
        self.unavailable_failed=0
        self.database_manager=None
        self.nodes=None
        self.history=[]
        self.final_summary=None

    def fingerprint(self):
        return self.config.connection.model_dump_json()+str(bool(self.password))

    def require_verified(self):
        if self.verified != self.fingerprint(): raise ValueError('Test and save the current connection first')

    def system(self,event,**data):
        entry=dict(timestamp=utc(),worker_id='SYSTEM',query_type='SYSTEM',event=event,**data)
        self.collector.log(entry)
        log.info(json.dumps(entry))

    async def initialize(self):
        saved=await self.store.get_config()
        if saved: self.config=Config.model_validate(saved)

    async def configure(self,config):
        async with self.lock:
            if self.state!='STOPPED' or (self.database_manager.task and not self.database_manager.task.done()): raise ValueError('Stop jobs before editing configuration')
            old=self.config.connection.model_dump()
            if config.connection.password is not None:
                self.password=config.connection.password.get_secret_value()
                self.verified=None
            elif any(getattr(config.connection,k)!=getattr(self.config.connection,k) for k in ('host','port','username')):
                self.password=''
                self.verified=None
            if config.connection.model_dump()!=old:
                self.verified=None
                if self.nodes:
                    self.nodes.last_primary=None
                    self.nodes.previous.pop('target',None)
            config.connection.password=None
            self.config=config
            await self.store.save_config(config)

    async def test(self,c):
        password=c.password.get_secret_value() if c.password is not None else self.password
        result=await test_connection(c,password)
        if result['success'] and c.model_dump()==self.config.connection.model_dump() and password==self.password:
            self.verified=self.fingerprint()
        return result

    async def start(self):
        async with self.lock:
            if self.state!='STOPPED': raise ValueError('Agent must be STOPPED')
            if self.database_manager.task and not self.database_manager.task.done(): raise ValueError('Wait for generator')
            self.require_verified()
            self.config=Config.model_validate(self.config.model_dump())
            self.state='STARTING'
            self.system('STARTING')
            try:
                conn=await connect(self.config.connection,self.password)
                try:
                    async with conn.cursor() as cur:
                        if self.config.workload.mode != 'INTEGRITY':
                            if self.config.workload.relational:
                                matches=[r for r in await self.store.databases() if r['name']==self.config.connection.database and r['status']=='READY' and r['host']==self.config.connection.host and r['port']==self.config.connection.port]
                                if not matches: raise ValueError('Relational mode requires a registered READY database')
                                await self.database_manager.owned(matches[0],conn,allow_server_change=True)
                                for table in NAMES:
                                    await cur.execute(f'SELECT id FROM {quote(table)} LIMIT 1')
                                    if not await cur.fetchone(): raise ValueError('Populate all managed tables before START')
                            else:
                                if self.config.connection.table == PROBE_TABLE:
                                    raise ValueError('Choose a separate table for ordinary workload; integrity table is reserved')
                                table=quote(self.config.connection.table)
                                if self.config.workload.auto_create_table:
                                    await cur.execute(f'CREATE TABLE IF NOT EXISTS {table} (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,payload LONGTEXT NOT NULL,payload_size INT UNSIGNED NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,INDEX idx_created_at(created_at)) ENGINE=InnoDB')
                                await cur.execute(f'SELECT id,payload,payload_size FROM {table} LIMIT 0')
                        if self.config.workload.mode in ('INTEGRITY','MIXED'):
                            if self.config.workload.auto_create_table:
                                await cur.execute(PROBE_DDL)
                            await cur.execute(f'SELECT probe_id,payload,revision FROM `{PROBE_TABLE}` LIMIT 0')
                finally: conn.close()
                last_error=None
                for attempt in range(self.config.connection.retries+1):
                    try:
                        self.pool=await create_pool(self.config,self.password)
                        break
                    except Exception as exc:
                        last_error=exc
                        if attempt==self.config.connection.retries: raise
                        await asyncio.sleep(min(2**attempt,self.config.workload.reconnect_timeout))
                self.dispatched=0
                self.session_id=uuid.uuid4().hex
                self.started_at=utc()
                self.ended_at=None
                self.collector=Collector()
                self.history=[]
                self.final_summary=None
                self.unavailable_from=None
                self.stop_event.clear()
                self.pause_event.set()
                self.connection_lifecycle=ConnectionLifecycle(self.config.workload.connection_refresh_every)
                self.limiter=RateLimiter(self.config.workload.rps,self.config.workload.rate_mode)
                await self.store.execute('INSERT INTO sessions VALUES (?,?,?,?,NULL)',(self.session_id,self.started_at,None,self.config.model_dump_json()))
                self.state='RUNNING'
                self.prometheus.start(self.session_id,self.config.connection.host)
                self.workers=[asyncio.create_task(Worker(self,f'worker-{i+1:02}').run()) for i in range(self.config.workload.workers)]
                for task in self.workers: task.add_done_callback(self.worker_done)
                self.system('RUNNING',session_id=self.session_id)
            except Exception as exc:
                await close_pool(self.pool)
                self.pool=None
                self.state='STOPPED'
                self.system('START_FAILED',error=safe_error(exc))
                raise ValueError(str(exc) if isinstance(exc,ValueError) else safe_error(exc)) from None
        return self.snapshot()

    def worker_done(self,task):
        if not task.cancelled() and task.exception() and not self.stop_event.is_set():
            self.state='ERROR'
            self.stop_event.set()
            self.pause_event.set()
            self.system('WORKER_FAILED',error=type(task.exception()).__name__)

    async def stop(self):
        async with self.lock:
            if self.state=='STOPPED': return self.snapshot()
            self.stop_event.set()
            self.pause_event.set()
            await asyncio.gather(*self.workers,return_exceptions=True)
            self.workers=[]
            await close_pool(self.pool)
            self.pool=None
            self.connection_lifecycle.clear()
            await self.store.queue.join()
            self.ended_at=utc()
            summary=self.collector.snapshot(self.expected_tables())
            summary.update(status='STOPPED',session_id=self.session_id,start_time=self.started_at,end_time=self.ended_at)
            await self.store.execute('UPDATE sessions SET ended=?,summary=? WHERE id=?',(self.ended_at,json.dumps(summary),self.session_id))
            self.final_summary=summary
            self.state='STOPPED'
            self.system('STOPPED',session_id=self.session_id)
            return summary

    async def pause(self):
        if self.state not in ('RUNNING','RECONNECTING'): raise ValueError('Agent is not running')
        self.pause_event.clear()
        self.state='PAUSED'
        self.system('PAUSED')

    async def resume(self):
        if self.state!='PAUSED': raise ValueError('Agent is not paused')
        self.state='RECONNECTING' if self.unavailable_from else 'RUNNING'
        self.pause_event.set()
        self.system('RESUMED')

    def disconnected(self):
        if not self.unavailable_from:
            self.unavailable_from=utc()
            self.unavailable_mono=time.monotonic()
            self.unavailable_failed=self.collector.counts['failed']
            self.system('CONNECTION_LOST')
        if self.state!='PAUSED': self.state='RECONNECTING'

    def recovered(self):
        if self.unavailable_from:
            self.collector.counts['reconnects']+=1
            self.prometheus.reconnects.inc()
            event=dict(event='CONNECTION_RESTORED',timestamp=utc(),database_unavailable_from=self.unavailable_from,
                database_available_from=utc(),total_downtime_ms=(time.monotonic()-self.unavailable_mono)*1000,
                failed_queries_during_failover=self.collector.counts['failed']-self.unavailable_failed,successful_queries_after_recovery=1)
            self.collector.failovers.append(event)
            self.system('CONNECTION_RESTORED',total_downtime_ms=event['total_downtime_ms'])
            self.unavailable_from=None
        elif self.collector.failovers:
            self.collector.failovers[-1]['successful_queries_after_recovery']+=1
        if self.state=='RECONNECTING': self.state='RUNNING'

    async def rebuild_pool(self, failed_pool):
        """Replace a pool invalidated by a database outage; concurrent workers share one rebuild."""
        async with self.pool_rebuild_lock:
            if self.stop_event.is_set():
                return False
            if self.pool is not failed_pool:
                return True  # another worker already replaced it
            if time.monotonic() < self.pool_rebuild_retry_after:
                return False  # don't let every worker hammer an unavailable endpoint

            old_pool = self.pool
            last_error = None
            attempts = self.config.connection.retries + 1
            for attempt in range(attempts):
                if self.stop_event.is_set():
                    return False
                try:
                    replacement = await create_pool(self.config, self.password)
                except Exception as exc:
                    last_error = exc
                    if attempt + 1 < attempts:
                        await asyncio.sleep(min(2 ** attempt, self.config.workload.reconnect_timeout))
                    continue

                if self.stop_event.is_set():
                    await close_pool(replacement)
                    return False

                self.pool = replacement
                self.pool_rebuild_retry_after = 0.0
                await close_pool(old_pool)
                self.system('CONNECTION_POOL_REBUILT')
                return True

            self.system('CONNECTION_POOL_REBUILD_FAILED', error=safe_error(last_error))
            self.pool_rebuild_retry_after = time.monotonic() + min(2.0, self.config.workload.reconnect_timeout)
            return False

    def expected_tables(self):
        w = self.config.workload
        normal = NAMES if w.relational else (self.config.connection.table,)
        if w.mode == 'INTEGRITY':
            return (PROBE_TABLE,)
        if w.mode == 'MIXED':
            return (*normal, PROBE_TABLE)
        return normal

    def snapshot(self):
        summary=self.collector.snapshot(self.expected_tables())
        if self.state=='STOPPED' and self.final_summary:
            summary=dict(self.final_summary)
        summary.update(state=self.state,session_id=self.session_id,start_time=self.started_at,
            active_connections=self.active_connections,pool_connections=self.pool.size if self.pool else 0,
            uptime=time.monotonic()-self.process_started,
            verified=self.verified==self.fingerprint(),password_saved=bool(self.password))
        return summary

    async def tick(self):
        if self.state in ('RUNNING','PAUSED','RECONNECTING'):
            w=self.config.workload
            expired=w.duration_seconds and time.monotonic()-self.collector.started>=w.duration_seconds
            completed=w.max_requests and self.dispatched>=w.max_requests
            if expired or completed:
                self.system('TEST_LIMIT_REACHED',reason='duration' if expired else 'requests')
                await self.stop()
        if self.store.error and self.state not in ('STOPPED','ERROR'):
            self.state='ERROR'
            self.stop_event.set()
            self.pause_event.set()
            self.system('STATISTICS_STORAGE_UNAVAILABLE',error=self.store.error)
        snapshot=self.snapshot()
        if self.session_id and self.state not in ('STOPPED','STARTING'):
            point={k:v for k,v in snapshot.items() if k not in ('tables','templates','failovers','slow_queries','latency_by_type')}
            point['timestamp']=utc()
            self.history.append(point)
            if len(self.history)>600: self.history.pop(0)
            try:
                await self.store.execute('INSERT INTO samples VALUES (?,?,?)',(self.session_id,point['timestamp'],json.dumps(point)))
                await self.store.execute('UPDATE sessions SET summary=? WHERE id=?',(json.dumps(snapshot),self.session_id))
            except Exception as exc:
                self.store.error=type(exc).__name__
                self.state='ERROR'
                self.stop_event.set()
                self.pause_event.set()
        return dict(metrics=snapshot,logs=list(self.collector.logs),history=self.history[-120:],
                    generator=self.database_manager.progress,nodes=self.nodes.public())
