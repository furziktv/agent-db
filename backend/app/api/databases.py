from fastapi import APIRouter, Request
from pydantic import BaseModel
from app.models.config import DatabaseRequest
from app.database.schema import TABLES
router=APIRouter()
class Confirmation(BaseModel):
    confirmation: str
@router.get('/databases')
async def databases(request: Request):
    return [{k:v for k,v in r.items() if k not in ('owner_token','server_uuid')} for r in await request.app.state.store.databases()]
@router.get('/databases/progress')
async def progress(request: Request): return request.app.state.agent.database_manager.progress
@router.post('/databases/create')
async def create(data: DatabaseRequest,request: Request): return await request.app.state.agent.database_manager.start(data)
@router.post('/databases/cancel')
async def cancel(request: Request):
    request.app.state.agent.database_manager.cancelled.set()
    return {'cancellation_requested':True,'message':'Current batch will finish; database will be preserved'}
@router.get('/databases/{id}/structure')
async def structure(id: str,request: Request):
    record=await request.app.state.store.database(id)
    return {'database':record['name'],'tables':[{'name':t.name,'columns':t.columns,'foreign_keys':t.refs,'indexes':t.indexes,'rows':record.get('rows_by_table',{}).get(t.name,0)} for t in TABLES]}
@router.post('/databases/{id}/use')
async def use(id: str,request: Request):
    await request.app.state.agent.database_manager.use(id)
    return {'ready':True}
@router.post('/databases/{id}/add')
async def add(id: str,data: DatabaseRequest,request: Request):
    record=await request.app.state.store.database(id)
    if record['name']!=data.name: raise ValueError('Database name differs from registry')
    return await request.app.state.agent.database_manager.start(data,id)
@router.post('/databases/{id}/clear')
async def clear(id: str,data: Confirmation,request: Request):
    await request.app.state.agent.database_manager.action(id,'clear',data.confirmation)
    return {'cleared':True}
@router.post('/databases/{id}/delete')
async def delete(id: str,data: Confirmation,request: Request):
    await request.app.state.agent.database_manager.action(id,'delete',data.confirmation)
    return {'deleted':True}
