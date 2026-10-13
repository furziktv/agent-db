import json
import os
import re
from starlette.background import BackgroundTask
from app.reports.export import snapshot, json_stream, csv_stream, docx_file
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import StreamingResponse, FileResponse
router=APIRouter()
@router.get('/reports')
async def reports(request: Request):
    rows=await request.app.state.store.fetch('SELECT id,started,ended,config,summary FROM sessions ORDER BY started DESC')
    return [dict(session_id=r[0],start_time=r[1],end_time=r[2],config=json.loads(r[3]),summary=json.loads(r[4]) if r[4] else {}) for r in rows]
async def get_report(store,id):
    rows=await store.fetch('SELECT id,started,ended,config,summary FROM sessions WHERE id=?',(id,))
    if not rows: raise HTTPException(404,'Session not found')
    r=rows[0]
    samples=await store.fetch('SELECT data FROM samples WHERE session_id=? ORDER BY rowid',(id,))
    # Bound interactive chart size; the full one-second series remains in SQLite.
    stride=max(1,len(samples)//2000)
    errors=await store.fetch("SELECT data FROM requests WHERE session_id=? AND json_extract(data,'$.success')=0 ORDER BY rowid DESC LIMIT 200",(id,))
    events=await store.fetch('SELECT data FROM events WHERE session_id=? ORDER BY rowid',(id,))
    return dict(session_id=id,start_time=r[1],end_time=r[2],config=json.loads(r[3]),summary=json.loads(r[4]) if r[4] else {},
        history=[json.loads(s[0]) for s in samples[::stride]],errors=[json.loads(e[0]) for e in errors],events=[json.loads(e[0]) for e in events])
@router.get('/reports/{id}')
async def report(id: str,request: Request): return await get_report(request.app.state.store,id)
@router.get('/reports/{id}/export')
async def export(id: str,request: Request,format: str='json'):
    if format not in ('json','csv','docx'):
        raise HTTPException(400,'Format must be csv, json or docx')
    data=await snapshot(request.app.state.store,id)
    filename=re.sub(r'[^A-Za-z0-9_-]','_',id)[:100] or 'session'
    headers={'Content-Disposition':f'attachment; filename="{filename}.{format}"'}
    if format=='docx':
        path=await docx_file(data)
        return FileResponse(path,media_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                            headers=headers,background=BackgroundTask(os.unlink,path))
    return StreamingResponse(json_stream(data) if format=='json' else csv_stream(data),
                             media_type='application/json' if format=='json' else 'text/csv',headers=headers)
