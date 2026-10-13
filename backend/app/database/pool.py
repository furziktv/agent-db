import asyncio
from contextlib import asynccontextmanager
import asyncmy
from app.database.connection import kwargs

async def create_pool(config, password):
    w = config.workload
    async with asyncio.timeout(config.connection.connection_timeout):
        pool = await asyncmy.create_pool(**kwargs(config.connection, password),
            minsize=w.pool_min_size, maxsize=w.pool_max_size, pool_recycle=300)
        return ManagedPool(pool)

async def close_pool(pool):
    if pool:
        pool.close()
        await pool.wait_closed()


class ConnectionLifecycle:
    """Count workload operations per physical connection, across all workers."""
    def __init__(self, refresh_every: int = 5) -> None:
        self.refresh_every = refresh_every
        self.uses: dict[object, int] = {}

    def complete(self, connection) -> bool:
        for tracked in tuple(self.uses):
            # asyncmy 0.2.10 leaves connected=True after close, but clears its reader.
            if tracked._reader is None:
                self.uses.pop(tracked, None)
        count = self.uses.get(connection, 0) + 1
        if count >= self.refresh_every:
            self.forget(connection)
            connection.close()
            return True
        self.uses[connection] = count
        return False

    def forget(self, connection) -> None:
        self.uses.pop(connection, None)

    def clear(self) -> None:
        self.uses.clear()


class ManagedPool:
    """Normalize asyncmy 0.2.10 close semantics before releasing a lease."""
    def __init__(self, pool) -> None:
        self.pool = pool

    @property
    def size(self) -> int:
        return self.pool.size

    @asynccontextmanager
    async def acquire(self):
        connection = await self.pool.acquire()
        try:
            yield connection
        finally:
            disconnected = connection._reader is None
            if disconnected:
                # close() clears the socket but not this flag in asyncmy 0.2.10.
                connection._connected = False
            await self.pool.release(connection)
            if disconnected:
                # Its disconnected release path does not notify waiting acquirers.
                await self.pool._wakeup()

    def close(self) -> None:
        self.pool.close()

    async def wait_closed(self) -> None:
        await self.pool.wait_closed()
