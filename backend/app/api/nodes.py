from fastapi import APIRouter, Request
from app.models.config import ConnectionConfig
router=APIRouter()
@router.get('/nodes')
async def nodes(request: Request): return request.app.state.agent.nodes.public()
@router.post('/nodes')
async def add(config: ConnectionConfig,request: Request):
    a=request.app.state.agent
    password=config.password.get_secret_value() if config.password else a.password
    return a.nodes.add(config,password)
@router.delete('/nodes/{id}')
async def remove(id: str,request: Request):
    request.app.state.agent.nodes.remove(id)
    return {'removed':True}
