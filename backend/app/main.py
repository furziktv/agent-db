import asyncio
import json
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response
from fastapi.exceptions import RequestValidationError
from app.api import config, agent, metrics, reports, nodes, databases
from app.agent.manager import AgentManager
from app.database.manager import DatabaseManager
from app.database.mysql_info import NodeMonitor
from app.reports.manager import Store

logging.basicConfig(level=logging.INFO,format='%(message)s')
logging.getLogger('asyncmy').setLevel(logging.CRITICAL)

@asynccontextmanager
async def lifespan(app):
    store=Store()
    await store.open()
    manager=AgentManager(store)
    await manager.initialize()
    manager.database_manager=DatabaseManager(manager,store)
    manager.nodes=NodeMonitor(manager)
    manager.nodes.task=asyncio.create_task(manager.nodes.poll())
    app.state.store,app.state.agent=store,manager
    app.state.clients=set()
    app.state.ready=True
    async def broadcaster():
        event_seq=0
        event_session=None
        while True:
            data=await manager.tick()
            if manager.session_id != event_session:
                event_seq=0
                event_session=manager.session_id
            for entry in list(manager.collector.logs):
                if entry['seq']>event_seq and entry['query_type']=='SYSTEM' and manager.session_id:
                    try:
                        await store.execute('INSERT INTO events VALUES (?,?,?)',(manager.session_id,entry['timestamp'],json.dumps(entry)))
                    except Exception as exc:
                        store.error=type(exc).__name__
            event_seq=manager.collector.log_seq
            app.state.live=data
            await asyncio.sleep(1)
    app.state.live=dict(metrics=manager.snapshot(),logs=[],history=[],generator=manager.database_manager.progress,nodes=[])
    broadcast_task=asyncio.create_task(broadcaster())
    try: yield
    finally:
        app.state.ready=False
        await manager.database_manager.shutdown()
        await manager.stop()
        await manager.nodes.close()
        broadcast_task.cancel()
        await asyncio.gather(broadcast_task,return_exceptions=True)
        await store.close()
        manager.password=''

app=FastAPI(title='MySQL Workload Agent',version='1.0.0',lifespan=lifespan)

@app.middleware('http')
async def local_write_guard(request: Request,call_next):
    # Browser writes require same-origin. Non-browser local API clients remain supported.
    origin=request.headers.get('origin')
    if request.method not in ('GET','HEAD','OPTIONS') and origin:
        from urllib.parse import urlsplit
        host=request.headers.get('x-forwarded-host',request.headers.get('host',''))
        if urlsplit(origin).netloc != host:
            return JSONResponse({'detail':'Cross-origin writes rejected'},status_code=403)
    try:
        return await call_next(request)
    except Exception as exc:
        logging.getLogger('agent').error(json.dumps({'event':'API_ERROR','type':type(exc).__name__}))
        return JSONResponse({'detail':'Internal operation failed; check system events'},status_code=500)

@app.exception_handler(RequestValidationError)
async def validation(request,exc):
    return JSONResponse({'detail':'; '.join('.'.join(map(str,e['loc']))+': '+e['msg'] for e in exc.errors())},status_code=422)

@app.exception_handler(ValueError)
async def invalid(request,exc): return JSONResponse({'detail':str(exc)},status_code=400)

@app.exception_handler(Exception)
async def failure(request,exc):
    logging.getLogger('agent').error(json.dumps({'event':'API_ERROR','type':type(exc).__name__}))
    return JSONResponse({'detail':'Internal operation failed; check system events'},status_code=500)

for router in (config.router,agent.router,metrics.router,reports.router,nodes.router,databases.router):
    app.include_router(router,prefix='/api')
@app.get('/health')
async def health(): return {'status':'ok'}
@app.get('/ready')
async def ready():
    if not app.state.ready or app.state.store.writer.done() or getattr(app.state.store,'error',None): return JSONResponse({'status':'unavailable'},status_code=503)
    await app.state.store.fetch('SELECT 1')
    return {'status':'ready'}
@app.get('/metrics')
async def prometheus(): return Response(app.state.agent.prometheus.render(app.state.agent),media_type='text/plain; version=0.0.4')
@app.websocket('/ws/live')
async def live(ws: WebSocket):
    origin=ws.headers.get('origin')
    if origin:
        from urllib.parse import urlsplit
        if urlsplit(origin).netloc != ws.headers.get('x-forwarded-host',ws.headers.get('host','')):
            await ws.close(code=1008)
            return
    await ws.accept()
    last_seq=0
    last_session=None
    async def receive_disconnect():
        while True:
            message=await ws.receive()
            if message['type']=='websocket.disconnect':
                return
    receiver=asyncio.create_task(receive_disconnect())
    try:
        while not receiver.done():
            data=app.state.live
            session=data['metrics'].get('session_id')
            if session!=last_session:
                last_seq=0
                last_session=session
            logs=[e for e in data['logs'] if e['seq']>last_seq]
            if logs: last_seq=logs[-1]['seq']
            async with asyncio.timeout(5):
                await ws.send_json(dict(data,logs=logs))
            await asyncio.wait({receiver},timeout=1)
    except (WebSocketDisconnect,RuntimeError,TimeoutError,OSError):
        pass
    finally:
        receiver.cancel()
        await asyncio.gather(receiver,return_exceptions=True)
