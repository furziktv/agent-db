import asyncio
import time
import uuid
from app.database.connection import test_connection
from app.monitoring.collector import utc

class NodeMonitor:
    def __init__(self,agent):
        self.agent=agent
        self.entries={}
        self.results={}
        self.previous={}
        self.last_primary=None
        self.task=None
        self.stop_event=asyncio.Event()

    def public(self):
        return list(self.results.values())

    def add(self,c,password):
        if len(self.entries)>=32: raise ValueError('Maximum 32 monitored nodes')
        id=uuid.uuid4().hex
        c.password=None
        self.entries[id]=(c,password)
        self.results[id]={'id':id,'host':c.host,'port':c.port,'success':False,'role':'UNKNOWN','status':'CHECKING'}
        return self.results[id]

    def remove(self,id):
        self.entries.pop(id,None)
        self.results.pop(id,None)
        self.previous.pop(id,None)

    async def poll(self):
        while not self.stop_event.is_set():
            entries=[('target',(self.agent.config.connection,self.agent.password)),*list(self.entries.items())]
            for id,(c,password) in entries:
                if id=='target' and self.agent.verified is None: continue
                info=await test_connection(c,password)
                now=time.monotonic()
                old=self.previous.get(id)
                rate=None
                if info.get('success'):
                    if old and info['Queries']>=old[1]: rate=(info['Queries']-old[1])/max(now-old[0],.001)
                    self.previous[id]=(now,info['Queries'])
                info.update(id=id,host=c.host,port=c.port,queries_per_sec=rate,status='CONNECTED' if info['success'] else 'DISCONNECTED')
                self.results[id]=info
                if id=='target' and info.get('role')=='PRIMARY':
                    identity=f"{info.get('hostname')}:{info.get('server_id')}"
                    if self.last_primary and identity!=self.last_primary:
                        event=dict(event='MASTER_CHANGED',old_master=self.last_primary,new_master=identity,timestamp=utc(),
                            database_unavailable_from=self.agent.unavailable_from,database_available_from=utc(),
                            total_downtime_ms=(now-self.agent.unavailable_mono)*1000 if self.agent.unavailable_from else 0,
                            failed_queries_during_failover=self.agent.collector.counts['failed']-self.agent.unavailable_failed if self.agent.unavailable_from else 0,
                            successful_queries_after_recovery=0)
                        self.agent.collector.failovers.append(event)
                        self.agent.system('MASTER_CHANGED',old_master=self.last_primary,new_master=identity)
                    self.last_primary=identity
            try: await asyncio.wait_for(self.stop_event.wait(),5)
            except TimeoutError: pass

    async def close(self):
        self.stop_event.set()
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task,return_exceptions=True)
        self.entries.clear()
