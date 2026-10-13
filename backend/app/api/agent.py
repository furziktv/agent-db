from fastapi import APIRouter, Request
from app.monitoring.collector import Collector
router=APIRouter()
@router.get('/status')
async def status(request: Request): return request.app.state.agent.snapshot()
@router.post('/agent/start')
async def start(request: Request): return await request.app.state.agent.start()
@router.post('/agent/stop')
async def stop(request: Request): return await request.app.state.agent.stop()
@router.post('/agent/pause')
async def pause(request: Request):
    await request.app.state.agent.pause()
    return request.app.state.agent.snapshot()
@router.post('/agent/resume')
async def resume(request: Request):
    await request.app.state.agent.resume()
    return request.app.state.agent.snapshot()
@router.post('/agent/reset')
async def reset(request: Request):
    a=request.app.state.agent
    if a.state!='STOPPED': raise ValueError('Reset statistics is allowed only when STOPPED')
    a.final_summary=None
    a.collector=Collector()
    a.history=[]
    a.session_id=None
    a.ended_at=None
    return a.snapshot()
