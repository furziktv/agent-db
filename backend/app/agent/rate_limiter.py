import asyncio
import time

class RateLimiter:
    """Global pacer or bounded token bucket; no catch-up burst after a stall."""
    def __init__(self, rate: float, mode: str = 'PACED'):
        self.rate = rate
        self.mode = mode
        self.lock = asyncio.Lock()
        self.next = time.monotonic()
        self.tokens = 0.0
        self.updated = self.next
        self.capacity = max(1.0, min(rate * .1, 100))

    async def acquire(self, stop_event: asyncio.Event | None = None) -> None:
        while True:
            async with self.lock:
                now = time.monotonic()
                if self.mode == 'PACED':
                    delay = self.next - now
                    if delay <= 0:
                        interval = 1 / self.rate
                        self.next = now + interval if now-self.next > interval else self.next+interval
                        return
                else:
                    self.tokens = min(self.capacity, self.tokens + (now-self.updated)*self.rate)
                    self.updated = now
                    if self.tokens >= 1:
                        self.tokens -= 1
                        return
                    delay = (1-self.tokens)/self.rate
            if stop_event:
                try:
                    await asyncio.wait_for(stop_event.wait(),timeout=max(.0001,delay))
                    return
                except TimeoutError: pass
            else:
                await asyncio.sleep(max(.0001, delay))
