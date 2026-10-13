import asyncio
import json
import os
import aiosqlite
from app.monitoring.collector import utc

class Store:
    def __init__(self):
        self.db = None
        self.queue = asyncio.Queue(maxsize=100000)
        self.writer = None
        self.error=None
        self.lock = asyncio.Lock()

    async def open(self):
        path = os.getenv('SQLITE_PATH','/data/agent.sqlite3')
        os.makedirs(os.path.dirname(path) or '.',exist_ok=True)
        self.db = await aiosqlite.connect(path)
        await self.db.executescript('''PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS settings (id INTEGER PRIMARY KEY, data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, started TEXT, ended TEXT, config TEXT, summary TEXT);
            CREATE TABLE IF NOT EXISTS samples (session_id TEXT, timestamp TEXT, data TEXT);
            CREATE INDEX IF NOT EXISTS samples_session ON samples(session_id);
            CREATE TABLE IF NOT EXISTS requests (session_id TEXT, timestamp TEXT, data TEXT);
            CREATE INDEX IF NOT EXISTS requests_session ON requests(session_id);
            CREATE TABLE IF NOT EXISTS events (session_id TEXT, timestamp TEXT, data TEXT);
            CREATE TABLE IF NOT EXISTS databases (id TEXT PRIMARY KEY, data TEXT);
        ''')
        await self.db.commit()
        # A process crash leaves a reviewable interrupted session, never an apparently running one.
        await self.db.execute("UPDATE sessions SET ended=?, summary=COALESCE(summary,?) WHERE ended IS NULL",
            (utc(),json.dumps({'status':'INTERRUPTED','reason':'process restarted'})))
        await self.db.commit()
        self.writer = asyncio.create_task(self.write_loop())

    async def execute(self, sql, args=()):
        async with self.lock:
            await self.db.execute(sql,args)
            await self.db.commit()

    async def fetch(self, sql, args=()):
        async with self.lock:
            async with self.db.execute(sql,args) as cur:
                return await cur.fetchall()

    async def write_loop(self):
        while True:
            item = await self.queue.get()
            batch = [item]
            for _ in range(999):
                try: batch.append(self.queue.get_nowait())
                except asyncio.QueueEmpty: break
            while True:
                try:
                    async with self.lock:
                        await self.db.executemany('INSERT INTO requests VALUES (?,?,?)',batch)
                        await self.db.commit()
                    self.error=None
                    for _ in batch: self.queue.task_done()
                    break
                except (aiosqlite.Error,OSError) as exc:
                    self.error=type(exc).__name__
                    async with self.lock:
                        await self.db.rollback()
                    await asyncio.sleep(1)

    async def request(self, session, event):
        # Backpressure limits throughput rather than dropping per-request records.
        await self.queue.put((session,event['timestamp'],json.dumps(event)))

    async def close(self):
        await self.queue.join()
        self.writer.cancel()
        await asyncio.gather(self.writer,return_exceptions=True)
        await self.db.close()

    async def get_config(self):
        rows = await self.fetch('SELECT data FROM settings WHERE id=1')
        return json.loads(rows[0][0]) if rows else None

    async def save_config(self, c):
        await self.execute('INSERT OR REPLACE INTO settings VALUES (1,?)',(json.dumps(c.model_dump()),))

    async def databases(self):
        return [json.loads(r[0]) for r in await self.fetch('SELECT data FROM databases ORDER BY rowid DESC')]

    async def database(self, id):
        rows = await self.fetch('SELECT data FROM databases WHERE id=?',(id,))
        if not rows: raise ValueError('Managed database not found')
        return json.loads(rows[0][0])

    async def save_database(self, data):
        await self.execute('INSERT OR REPLACE INTO databases VALUES (?,?)',(data['id'],json.dumps(data)))
