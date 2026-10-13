from app.agent.integrity import probe, PROBE_TABLE
import asyncio
import random
import re
import time
from app.agent.generator import payload
from app.agent.templates import TEMPLATES, UPDATE_FIELDS
from app.database.schema import BY_NAME, insert_sql
from app.database.data import row_values
from app.database.connection import CONNECTION_CODES, safe_error
from app.models.config import quote
from app.monitoring.collector import utc

async def seek(cur, table, rng):
    await cur.execute(f'SELECT MAX(id) FROM {quote(table)}')
    maximum = (await cur.fetchone())[0]
    if not maximum: return None
    await cur.execute(f'SELECT id FROM {quote(table)} WHERE id >= %s ORDER BY id LIMIT 1',(rng.randint(1,maximum),))
    row = await cur.fetchone()
    return row[0] if row else None

class Worker:
    def __init__(self, manager, id):
        self.m, self.id, self.rng = manager, id, random.Random()
        self.table_stats={}

    def choose(self):
        w = self.m.config.workload
        if w.mode == 'INTEGRITY' or (w.mode == 'MIXED' and self.rng.random()*100 < w.integrity_percent):
            return 'INTEGRITY', 'integrity', None
        if not w.relational:
            op = self.rng.choices(('SELECT','INSERT','UPDATE'),weights=(w.select_percent,w.insert_percent,w.update_percent))[0]
            return op, 'simple' if op=='SELECT' else op.lower(), None
        kind = self.rng.choices(('simple','join','aggregate','insert','update','transaction'),
             weights=(w.simple_percent,w.join_percent,w.aggregate_percent,w.relational_insert_percent,w.relational_update_percent,w.transaction_percent))[0]
        choices = [t for t in TEMPLATES if t.kind==kind]
        if kind=='simple' and w.complexity=='MEDIUM':
            choices += [t for t in TEMPLATES if t.kind=='join' and len(t.tables)==2]
        elif kind=='simple' and w.complexity=='HIGH':
            choices = [t for t in TEMPLATES if t.kind in ('join','aggregate') and t.complexity=='HIGH']
        elif kind=='simple' and w.complexity=='RANDOM':
            choices += [t for t in TEMPLATES if t.kind in ('join','aggregate')]
        # Boost stale tables within the selected class; preserve configured class probabilities.
        now = time.monotonic()
        weights = [1 + min(30,max(now-self.m.collector.last_used.get(table,self.m.collector.started) for table in t.tables)/5) for t in choices]
        template = self.rng.choices(choices,weights=weights)[0]
        op = 'SELECT' if template.kind in ('simple','join','aggregate') else ('TRANSACTION' if kind=='transaction' else kind.upper())
        return op, template.kind, template

    async def execute(self,conn,op,kind,template,text,rows):
        c, w = self.m.config.connection,self.m.config.workload
        counters = dict(rows_read=0,rows_inserted=0,rows_updated=0,bytes_read=0,bytes_generated=0,statements=0,sql_counts={'SELECT':0,'INSERT':0,'UPDATE':0})
        self.operation_counters = counters
        async with conn.cursor() as cur:
            async def run(sql,args=()):
                operation=sql.split()[0].upper()
                touched=set(re.findall(r'(?i)\b(?:FROM|JOIN|INTO|UPDATE)\s+`?([a-z_]+)`?',sql))
                before=time.perf_counter()
                try:
                    async with asyncio.timeout(w.query_timeout):
                        await cur.execute(sql,args)
                    counters['statements'] += 1
                    if operation in counters['sql_counts']:
                        counters['sql_counts'][operation] += 1
                finally:
                    elapsed=(time.perf_counter()-before)*1000
                    for table_name in touched:
                        values=self.table_stats.setdefault(table_name,{})
                        values[operation]=values.get(operation,0)+1
                        key=operation+'_latency_sum'
                        values[key]=values.get(key,0)+elapsed
                return cur.rowcount
            async def read(sql,args):
                await run(sql,args)
                async with asyncio.timeout(w.query_timeout):
                    result = await cur.fetchall()
                counters['rows_read'] += len(result)
                counters['bytes_read'] += sum(len(v) if isinstance(v,bytes) else len(str(v).encode()) if v is not None else 0 for row in result for v in row)
                return result
            async def insert(table, values=None):
                values = values or await row_values(table,cur,self.rng,text)
                await run(insert_sql(BY_NAME[table]),values)
                counters['rows_inserted'] += cur.rowcount
                counters['bytes_generated'] += sum(len(v.encode()) for v in values if isinstance(v,str))
                return cur.lastrowid
            async def update(table,id):
                if id is None: return
                field,value = UPDATE_FIELDS[table]
                value = text if value is None else value
                await run(f'UPDATE {quote(table)} SET {quote(field)}=%s WHERE id=%s LIMIT 1',(value,id))
                counters['rows_updated'] += cur.rowcount
                if isinstance(value,str): counters['bytes_generated'] += len(value.encode())
            if kind == 'integrity':
                await probe(run, read, counters, w, self.rng, self.m.limiter, text)
                return counters
            if not w.relational:
                table = quote(c.table)
                if op=='INSERT':
                    await run(f'INSERT INTO {table}(payload,payload_size) VALUES (%s,%s)',(text,len(text)))
                    counters.update(rows_inserted=cur.rowcount,bytes_generated=len(text))
                elif op=='UPDATE':
                    id = await seek(cur,c.table,self.rng)
                    if id is not None:
                        await run(f'UPDATE {table} SET payload=%s,payload_size=%s WHERE id=%s LIMIT 1',(text,len(text),id))
                        counters.update(rows_updated=cur.rowcount,bytes_generated=len(text))
                else:
                    id = await seek(cur,c.table,self.rng)
                    await read(f'SELECT id,payload,payload_size FROM {table} WHERE id >= %s ORDER BY id LIMIT %s',(id or 1,rows))
                return counters
            table = template.tables[0]
            if kind in ('simple','join','aggregate'):
                id = await seek(cur,table,self.rng) or 1
                if kind=='aggregate' or template.name.startswith('range_'):
                    args = (id,id+rows*2,rows)
                else: args = (id,rows)
                sql = template.sql
                if w.index_mode=='MIXED' and kind=='simple' and self.rng.random()<.2:
                    # Deliberately less optimal, but bounded to an indexed ID window.
                    sql = f'SELECT * FROM {quote(table)} WHERE id BETWEEN %s AND %s AND MOD(id,2)=0 ORDER BY id LIMIT %s'
                    args=(id,id+rows*4,rows)
                await read(sql,args)
            elif kind=='insert':
                await insert(table)
            elif kind=='update':
                await update(table,await seek(cur,table,self.rng))
            else:
                await conn.begin()
                try:
                    if template.name=='transaction_user':
                        user = await insert('users')
                        await insert('user_profiles',(user,'+0-000-000','1990-01-01',text,'{}'))
                        await insert('addresses',(user,'Testland','Demo City','Synthetic Street','00000','shipping'))
                    elif template.name=='transaction_review':
                        await insert('reviews')
                    elif template.name=='transaction_status':
                        order = await seek(cur,'orders',self.rng)
                        await run('SELECT user_id FROM orders WHERE id=%s FOR UPDATE',(order,))
                        user = (await cur.fetchone())[0]
                        await update('orders',order)
                        await insert('audit_events',(user,order,'order_status',text))
                    else:
                        values = list(await row_values('orders',cur,self.rng,text))
                        inventory = await seek(cur,'product_inventory',self.rng)
                        await run('SELECT product_id FROM product_inventory WHERE id=%s FOR UPDATE',(inventory,))
                        product = (await cur.fetchone())[0]
                        await run('SELECT price FROM products WHERE id=%s',(product,))
                        price = (await cur.fetchone())[0]
                        quantity = self.rng.randint(1,3)
                        total = price*quantity
                        values[-1]=total
                        order = await insert('orders',tuple(values))
                        await insert('order_items',(order,product,quantity,price))
                        await run('UPDATE product_inventory SET quantity=quantity-%s WHERE id=%s AND quantity >= %s LIMIT 1',(quantity,inventory,quantity))
                        if cur.rowcount != 1: raise ValueError('No inventory available; transaction rolled back')
                        counters['rows_updated'] += cur.rowcount
                        await insert('payments',(order,'test-card',total,'pending','TEST-'+str(order)))
                    await conn.commit()
                except BaseException:
                    await conn.rollback()
                    raise
            return counters

    async def run(self):
        m,w = self.m,self.m.config.workload
        backoff = 1.0
        while not m.stop_event.is_set():
            await m.pause_event.wait()
            if m.stop_event.is_set(): break
            await m.limiter.acquire(m.stop_event)
            if m.stop_event.is_set() or not m.pause_event.is_set(): continue
            if w.max_requests and m.dispatched >= w.max_requests:
                m.stop_event.set()
                m.pause_event.set()
                break
            m.dispatched += 1
            op,kind,t = self.choose()
            is_integrity = kind == 'integrity'
            # SELECT does not generate unused large payloads.
            text = payload(w,self.rng) if op!='SELECT' else ''
            rows = self.rng.randint(w.min_rows,w.max_rows)
            started=time.perf_counter()
            acquired=started
            event = dict(timestamp=utc(),worker_id=self.id,query_type=op,query_class=kind,
                template='write_read_update_read' if is_integrity else t.name if t else 'single_'+op.lower(),tables=[PROBE_TABLE] if is_integrity else list(t.tables) if t else [m.config.connection.table],
                database_host=m.config.connection.host,database_port=m.config.connection.port,
                database_name=m.config.connection.database,payload_size=len(text),rows_read=0,rows_affected=0,
                rows_inserted=0,rows_updated=0,bytes_generated=0,bytes_read=0,success=False,
                error_message=None,mysql_error_code=None,connection_time_ms=0,sql_counts={})
            conn=None
            request_pool=m.pool
            self.table_stats={}
            self.operation_counters={}
            try:
                cycle_timeout = 4*w.query_timeout+4*w.workers/w.rps if is_integrity else w.query_timeout
                async with asyncio.timeout(cycle_timeout):
                    async with request_pool.acquire() as conn:
                        acquired=time.perf_counter()
                        event['connection_time_ms']=(acquired-started)*1000
                        m.active_connections += 1
                        try:
                            result=await self.execute(conn,op,kind,t,text,rows)
                            event.update(result,success=not result.get('integrity_lost_writes'))
                            if result.get('integrity_lost_writes'):
                                event.update(error_kind='integrity_errors', error_message='Write verification failed: '+', '.join(d['stage']+': '+d['reason'] for d in result['integrity_details']))
                            event['connection_refreshed']=m.connection_lifecycle.complete(conn)
                        except BaseException:
                            # Discard before pool release; a closed pooled reader must never be reused.
                            m.connection_lifecycle.forget(conn)
                            conn.close()
                            raise
                        finally: m.active_connections-=1
                backoff=1
                m.recovered()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                code=exc.args[0] if exc.args and isinstance(exc.args[0],int) else None
                event.update(self.operation_counters)
                if is_integrity:
                    event['integrity_inconclusive'] = max(0, 2-event.get('integrity_checks',0))
                event.update(error_message=safe_error(exc),mysql_error_code=code)
                # asyncmy can surface a dead socket from a recycled pooled connection
                # as AttributeError (rather than a MySQL/OSError). Workload SQL errors
                # have concrete MySQL error codes; this driver-level shape is treated
                # as transport failure so the pool is rebuilt instead of hot-looping.
                transport_error = (code in CONNECTION_CODES or
                    isinstance(exc,(OSError,ConnectionError,EOFError,AttributeError)))
                event['error_kind']='timeouts' if isinstance(exc,TimeoutError) else 'connection_errors' if transport_error else 'sql_errors'
                if event['error_kind'] in ('connection_errors','timeouts'):
                    m.disconnected()
                    await m.rebuild_pool(request_pool)
            event['table_stats']=self.table_stats
            event['execution_time_ms']=(time.perf_counter()-acquired)*1000
            event['rows_affected']=event['rows_inserted']+event['rows_updated']
            event['slow']=event['execution_time_ms']>=w.slow_threshold_ms
            m.collector.record(event)
            m.prometheus.record(event)
            await m.store.request(m.session_id,event)
            if event.get('error_kind') in ('connection_errors','timeouts'):
                try: await asyncio.wait_for(m.stop_event.wait(),timeout=backoff)
                except TimeoutError: pass
                backoff=min(w.reconnect_timeout,backoff*2)
            elif w.delay_ms:
                try: await asyncio.wait_for(m.stop_event.wait(),timeout=w.delay_ms/1000)
                except TimeoutError: pass
