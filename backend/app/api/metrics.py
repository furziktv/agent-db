from fastapi import APIRouter, Request
router=APIRouter()
@router.get('/metrics/current')
async def current(request: Request): return request.app.state.agent.snapshot()
