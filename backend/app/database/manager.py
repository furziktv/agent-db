import asyncio
import json
import random
import re
import time
import uuid
from app.database.connection import connect, safe_error
from app.database.schema import TABLES, NAMES, ddl, insert_sql
from app.database.data import row_values
from app.models.config import quote
from app.monitoring.collector import utc

class DatabaseManager:
    def __init__(self, agent, store):
        self.agent, self.store = agent, store
        self.task = None
        self.cancelled = asyncio.Event()
        self.progress = {'state':'IDLE'}
        self.lock = asyncio.Lock()

    def update(self, state, **data):
        self.progress = dict(self.progress,state=state,**data)
        self.agent.system('GENERATOR_'+state, **data)

    async def check_privileges(self, conn, name):
        async with conn.cursor() as cur:
            await cur.execute('SHOW GRANTS FOR CURRENT_USER')
            grants = [r[0] for r in await cur.fetchall()]
        allowed = set()
        for grant in grants:
            match = re.match(r'GRANT (.+) ON (.+) TO ',grant)
            if not match: continue
            scope = match[2].replace('`','')
            # Literal database names only, or global grants. Wildcard grants require a real CREATE probe.
            if scope not in ('*.*',name+'.*'): continue
            privileges = match[1].split(', ')
            if 'ALL PRIVILEGES' in privileges:
                allowed.update(('CREATE','ALTER','INDEX','INSERT','SELECT','UPDATE','REFERENCES'))
            else: allowed.update(privileges)
        needed = {'CREATE','ALTER','INDEX','INSERT','SELECT','UPDATE','REFERENCES'}
        if not needed <= allowed:
            raise ValueError('Required direct grants for this database: '+', '.join(sorted(needed-allowed)))

    async def start(self, req, add_id=None):
        async with self.lock:
            if self.task and not self.task.done(): raise ValueError('Generation already running')
            if self.agent.state != 'STOPPED': raise ValueError('Stop workload before managing databases')
            self.agent.require_verified()
            self.cancelled.clear()
            self.progress = {'state':'CHECKING_CONNECTION','percent':0,'rows_done':0,'rows_total':req.records,'name':req.name}
            self.task = asyncio.create_task(self.generate(req,add_id))
        return self.progress

    async def owned(self, record: dict, conn, *, allow_server_change: bool = False) -> None:
        if record['host'] != self.agent.config.connection.host or record['port'] != self.agent.config.connection.port:
            raise ValueError('Connect to the registered server first')
        async with conn.cursor() as cur:
            await cur.execute('SELECT @@server_uuid')
            current_uuid = (await cur.fetchone())[0]
            await cur.execute(
                f'SELECT token FROM {quote(record["name"])}.`_agent_owner` WHERE token=%s',
                (record['owner_token'],),
            )
            marker = await cur.fetchone()
            if not marker or marker[0] != record['owner_token']:
                raise ValueError('Ownership marker mismatch: this database is not the registered agent database')
            if current_uuid != record['server_uuid']:
                if not allow_server_change:
                    raise ValueError('Server identity differs from registry; destructive actions require the original server')
                self.agent.system(
                    'MANAGED_DATABASE_SERVER_CHANGED',
                    database=record['name'],
                    original_server_uuid=record['server_uuid'],
                    current_server_uuid=current_uuid,
                )

    async def generate(self, req, add_id=None):
        conn = None
        record = None
        try:
            conn = await connect(self.agent.config.connection,self.agent.password,False)
            async with conn.cursor() as cur:
                if add_id:
                    record = await self.store.database(add_id)
                    await self.owned(record,conn)
                    if record['status'] != 'READY': raise ValueError('Only READY databases can be extended')
                else:
                    await self.check_privileges(conn,req.name)
                    await cur.execute('SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=%s',(req.name,))
                    if await cur.fetchone(): raise ValueError('Database already exists; creation refused')
                    await cur.execute('SELECT @@server_uuid')
                    server_uuid = (await cur.fetchone())[0]
                    record = dict(id=uuid.uuid4().hex,name=req.name,host=self.agent.config.connection.host,
                        port=self.agent.config.connection.port,created_at=utc(),status='CREATING',tables=list(NAMES),
                        rows=0,size_bytes=0,server_uuid=server_uuid,owner_token=uuid.uuid4().hex,
                        foreign_keys=req.create_foreign_keys,indexes=req.create_indexes)
                    # Register intention before DDL; cleanup additionally requires the marker.
                    await self.store.save_database(record)
                    self.update('CREATING_DATABASE',percent=0)
                    await cur.execute(f'CREATE DATABASE {quote(req.name)} CHARACTER SET utf8mb4')
                    await cur.execute(f'USE {quote(req.name)}')
                    await cur.execute('CREATE TABLE `_agent_owner` (token CHAR(32) PRIMARY KEY) ENGINE=InnoDB')
                    await cur.execute('INSERT INTO `_agent_owner` VALUES (%s)',(record['owner_token'],))
                    self.update('CREATING_SCHEMA')
                    for table in TABLES:
                        await cur.execute(ddl(table,req.create_foreign_keys))
                await cur.execute(f'USE {quote(req.name)}')
                refs = {}
                for t in TABLES:
                    await cur.execute(f'SELECT id FROM {quote(t.name)} ORDER BY id DESC LIMIT 10000')
                    refs[t.name] = [r[0] for r in await cur.fetchall()]
                total = req.records if req.generate_initial_data else 0
                counts = {t.name:total//len(TABLES)+(i<total%len(TABLES)) for i,t in enumerate(TABLES)}
                done = 0
                rng = random.Random()
                # Bounded batches by payload bytes as well as row count.
                batch_size = min(req.batch_size,max(1,2_000_000//max(req.payload_size+512,1)))
                for table in TABLES:
                    self.update('GENERATING_DATA',table=table.name,table_percent=0)
                    target = counts[table.name]
                    for offset in range(0,target,batch_size):
                        if self.cancelled.is_set(): raise asyncio.CancelledError()
                        rows = []
                        for i in range(min(batch_size,target-offset)):
                            text = ''.join(rng.choices('abcdefghijklmnopqrstuvwxyz0123456789 ',k=req.payload_size))
                            rows.append(await row_values(table.name,cur,rng,text,refs))
                        await conn.begin()
                        try:
                            await cur.executemany(insert_sql(table),rows)
                            await conn.commit()
                        except BaseException:
                            await conn.rollback()
                            raise
                        done += len(rows)
                        await cur.execute(f'SELECT id FROM {quote(table.name)} ORDER BY id DESC LIMIT 10000')
                        refs[table.name] = [r[0] for r in await cur.fetchall()]
                        self.progress.update(rows_done=done,percent=round(done/max(total,1)*100,2),table_percent=min(100,(offset+len(rows))/max(target,1)*100))
                        await asyncio.sleep(0)
                    if not add_id and table.name == 'categories' and len(refs['categories']) > 1:
                        root=min(refs['categories'])
                        await cur.execute('UPDATE categories SET parent_id=%s WHERE id>%s',(root,root))
                if self.cancelled.is_set(): raise asyncio.CancelledError()
                # Reconcile generated item totals and payment amounts.
                await cur.execute('UPDATE orders o JOIN (SELECT order_id,SUM(quantity*price) total FROM order_items GROUP BY order_id) x ON x.order_id=o.id SET o.total_amount=x.total')
                await cur.execute('UPDATE payments p JOIN orders o ON o.id=p.order_id SET p.amount=o.total_amount')
                if not add_id and req.create_indexes:
                    self.update('CREATING_INDEXES')
                    for t in TABLES:
                        if self.cancelled.is_set(): raise asyncio.CancelledError()
                        for column in t.indexes:
                            await cur.execute(f'CREATE INDEX {quote("idx_"+t.name+"_"+column)} ON {quote(t.name)} ({quote(column)})')
                if self.cancelled.is_set(): raise asyncio.CancelledError()
                self.update('VERIFYING')
                result = await self.verify(conn,record)
                record.update(result,status='READY')
                await self.store.save_database(record)
                self.agent.config.connection.database = req.name
                self.agent.config.workload.relational = True
                await self.store.save_config(self.agent.config)
                self.agent.verified = self.agent.fingerprint()
                self.update('READY',percent=100,database_id=record['id'],message='DATABASE READY FOR TESTING')
        except asyncio.CancelledError:
            if record:
                record['status'] = 'CANCELLED'
                await self.store.save_database(record)
            self.update('CANCELLED',message='Partial database preserved. Delete it with a separate confirmation.')
        except Exception as exc:
            if record:
                record['status'] = 'FAILED'
                await self.store.save_database(record)
            self.update('FAILED',error=str(exc) if isinstance(exc,ValueError) else safe_error(exc))
        finally:
            if conn: conn.close()

    async def verify(self, conn, record):
        rows_by_table = {}
        async with conn.cursor() as cur:
            for table in TABLES:
                await cur.execute(f'SELECT COUNT(*) FROM {quote(table.name)}')
                rows_by_table[table.name] = (await cur.fetchone())[0]
                await cur.execute(f'SHOW INDEX FROM {quote(table.name)}')
                indexes = await cur.fetchall()
                if not any(r[2]=='PRIMARY' for r in indexes): raise ValueError('Missing PRIMARY KEY')
                if record['indexes']:
                    for col in table.indexes:
                        if not any(r[4]==col for r in indexes): raise ValueError('Missing index: '+col)
                for col,parent in table.refs:
                    if record['foreign_keys']:
                        await cur.execute('SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s AND REFERENCED_TABLE_NAME=%s',(record['name'],table.name,col,parent))
                        if not (await cur.fetchone())[0]: raise ValueError('Missing foreign key')
                    await cur.execute(f'SELECT COUNT(*) FROM {quote(table.name)} t LEFT JOIN {quote(parent)} p ON t.{quote(col)}=p.id WHERE t.{quote(col)} IS NOT NULL AND p.id IS NULL')
                    if (await cur.fetchone())[0]: raise ValueError('Foreign key relationship invalid')
            await cur.execute('SELECT u.id,p.id FROM users u LEFT JOIN user_profiles p ON p.user_id=u.id LIMIT 1')
            await cur.fetchall()
            # Real INSERT and UPDATE probes, rolled back; privileges alone are insufficient.
            await conn.begin()
            try:
                await cur.execute("INSERT INTO users(username,email,status) VALUES (%s,%s,'active')",('verification_'+uuid.uuid4().hex,'verification@example.invalid'))
                await cur.execute('UPDATE users SET status=%s WHERE id=%s',('inactive',cur.lastrowid))
            finally: await conn.rollback()
            await cur.execute('SELECT COALESCE(SUM(DATA_LENGTH+INDEX_LENGTH),0) FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s',(record['name'],))
            size = int((await cur.fetchone())[0])
        return dict(rows=sum(rows_by_table.values()),rows_by_table=rows_by_table,size_bytes=size,verified_at=utc())

    async def action(self, id, action, confirmation):
        if action not in ('delete', 'clear'):
            raise ValueError('Unknown action')
        async with self.lock:
            if self.agent.state != 'STOPPED' or (self.task and not self.task.done()): raise ValueError('Stop workload and generator first')
            record = await self.store.database(id)
            if confirmation != record['name']: raise ValueError('Type exact database name to confirm')
            conn = await connect(self.agent.config.connection,self.agent.password,False)
            try:
                await self.owned(record, conn, allow_server_change=True)
                async with conn.cursor() as cur:
                    # Check the same physical connection that will execute the operation.
                    await cur.execute('SELECT @@read_only, @@super_read_only')
                    read_only, super_read_only = await cur.fetchone()
                    if read_only or super_read_only:
                        raise ValueError('Current server is read-only; connect to the writable primary before clearing or deleting a test database')
                    if action == 'delete':
                        await cur.execute(f'DROP DATABASE {quote(record["name"])}')
                        record['status'] = 'DELETED'
                    elif action == 'clear':
                        await cur.execute(f'USE {quote(record["name"])}')
                        await conn.begin()
                        try:
                            # Self-referencing categories are unlinked before child-first deletion.
                            await cur.execute('UPDATE categories SET parent_id=NULL WHERE parent_id IS NOT NULL')
                            for t in reversed(TABLES): await cur.execute(f'DELETE FROM {quote(t.name)}')
                            await conn.commit()
                        except BaseException:
                            await conn.rollback()
                            raise
                        record.update(rows=0,rows_by_table={t:0 for t in NAMES},status='READY')
                    else: raise ValueError('Unknown action')
                await self.store.save_database(record)
                return record
            finally: conn.close()

    async def use(self,id):
        if self.agent.state != 'STOPPED' or (self.task and not self.task.done()): raise ValueError('Stop running jobs first')
        record = await self.store.database(id)
        if record['status'] != 'READY': raise ValueError('Database is not READY')
        conn = await connect(self.agent.config.connection,self.agent.password,False)
        try: await self.owned(record,conn,allow_server_change=True)
        finally: conn.close()
        self.agent.config.connection.database=record['name']
        self.agent.config.workload.relational=True
        self.agent.verified=self.agent.fingerprint()
        await self.store.save_config(self.agent.config)
        return record

    async def shutdown(self):
        self.cancelled.set()
        if self.task and not self.task.done(): await self.task
