import asyncio
import time
import asyncmy
from asyncmy.cursors import DictCursor
from app.models.config import ConnectionConfig

CONNECTION_CODES = {1040, 1042, 1043, 1047, 1129, 1130, 1158, 1159, 1160, 1161, 2002, 2003, 2006, 2013, 2055}

def safe_error(exc: BaseException, secret: str = '') -> str:
    code = exc.args[0] if exc.args and isinstance(exc.args[0], int) else None
    # Driver messages can contain SQL payloads and credentials. Never retain raw messages.
    if isinstance(exc, TimeoutError):
        return 'Operation timed out'
    messages = {1045:'Authentication denied',1049:'Unknown database',1142:'Insufficient privileges',
                1146:'Table not found',1062:'Duplicate key',1452:'Foreign key constraint failed',
                1290:'Server is read-only',1213:'Transaction deadlock',1205:'Lock wait timeout'}
    return messages.get(code, 'Connection unavailable' if code in CONNECTION_CODES else
                        f'{type(exc).__name__}' + (f' (MySQL {code})' if code else ''))

def kwargs(c: ConnectionConfig, password: str, database: bool = True) -> dict:
    result = dict(host=c.host, port=c.port, user=c.username, password=password,
                  connect_timeout=c.connection_timeout, autocommit=True, charset='utf8mb4')
    if database:
        result['db'] = c.database
    return result

class ServiceConnection:
    def __init__(self, connection, gate):
        self.connection, self.gate, self.closed = connection, gate, False
    def __getattr__(self,name):
        return getattr(self.connection,name)
    def close(self):
        if not self.closed:
            self.closed=True
            self.connection.close()
            self.gate.release()

# Reserve two non-pool connections for metadata, probes and generator.
service_gate = asyncio.Semaphore(2)

async def connect(c: ConnectionConfig, password: str, database: bool = True):
    async with asyncio.timeout(c.connection_timeout):
        await service_gate.acquire()
        try:
            connection=await asyncmy.connect(**kwargs(c,password,database))
            return ServiceConnection(connection,service_gate)
        except BaseException:
            service_gate.release()
            raise

async def server_info(conn) -> dict:
    async with conn.cursor(DictCursor) as cur:
        await cur.execute('SELECT VERSION() AS version, @@hostname AS hostname, @@server_id AS server_id, @@read_only AS read_only')
        info = dict(await cur.fetchone())
        try:
            await cur.execute('SELECT @@super_read_only AS super_read_only')
            info.update(await cur.fetchone())
        except asyncmy.errors.MySQLError:
            info['super_read_only'] = None
        await cur.execute("SHOW GLOBAL STATUS WHERE Variable_name IN ('Uptime','Threads_connected','Threads_running','Queries')")
        info.update({r['Variable_name']: int(r['Value']) for r in await cur.fetchall()})
        replica = None
        observable = False
        for sql in ('SHOW REPLICA STATUS','SHOW SLAVE STATUS'):
            try:
                await cur.execute(sql)
                replica = await cur.fetchone()
                observable = True
                break
            except asyncmy.errors.MySQLError:
                continue
        info['replication_observable'] = observable
        info['role'] = 'REPLICA' if replica else ('PRIMARY' if observable and not info['read_only'] and not info['super_read_only'] else 'UNKNOWN')
        info['replication'] = {} if not replica else {
            'io_thread': replica.get('Replica_IO_Running', replica.get('Slave_IO_Running')),
            'sql_thread': replica.get('Replica_SQL_Running', replica.get('Slave_SQL_Running')),
            'lag_seconds': replica.get('Seconds_Behind_Source', replica.get('Seconds_Behind_Master')),
            'source_host': replica.get('Source_Host', replica.get('Master_Host'))}
        try:
            await cur.execute("SELECT MEMBER_ROLE FROM performance_schema.replication_group_members WHERE MEMBER_ID=@@server_uuid")
            row = await cur.fetchone()
            if row:
                info['role'] = 'PRIMARY' if row['MEMBER_ROLE'] == 'PRIMARY' else 'REPLICA'
        except asyncmy.errors.MySQLError:
            pass
        return info

async def test_connection(c: ConnectionConfig, password: str, database: bool = False) -> dict:
    started = time.perf_counter()
    conn = None
    try:
        conn = await connect(c, password, database)
        ms = (time.perf_counter() - started) * 1000
        async with asyncio.timeout(c.connection_timeout):
            info = await server_info(conn)
        return dict(success=True, connection_time_ms=ms, **info)
    except Exception as exc:
        return dict(success=False, connection_time_ms=(time.perf_counter()-started)*1000, error=safe_error(exc))
    finally:
        if conn:
            conn.close()
