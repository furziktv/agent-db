from fastapi import APIRouter, Request
from app.models.config import Config, ConnectionConfig, LIMITS
router=APIRouter()
@router.get('/config')
async def config(request: Request):
    a=request.app.state.agent
    return dict(config=a.config.model_dump(),limits=LIMITS,password_saved=bool(a.password),verified=a.verified==a.fingerprint())
@router.post('/config')
async def save(config: Config,request: Request):
    await request.app.state.agent.configure(config)
    return {'saved':True,'password_saved':bool(request.app.state.agent.password)}
@router.post('/connection/test')
async def test(config: ConnectionConfig,request: Request):
    return await request.app.state.agent.test(config)

@router.get('/connection/databases')
async def available_databases(request: Request) -> dict:
    from app.database.connection import connect, safe_error
    from app.models.config import identifier
    agent=request.app.state.agent
    agent.require_verified()
    connection=None
    try:
        connection=await connect(agent.config.connection,agent.password,False)
        import asyncio
        async with asyncio.timeout(agent.config.connection.connection_timeout):
            async with connection.cursor() as cursor:
                await cursor.execute('SHOW DATABASES')
                rows=await cursor.fetchall()
        registered={d['name']:d for d in await agent.store.databases()
            if d['host']==agent.config.connection.host and d['port']==agent.config.connection.port and d['status']=='READY'}
        databases=[]
        for row in rows:
            name=row[0]
            if name.lower() in {'mysql','sys','information_schema','performance_schema'}: continue
            try: identifier(name)
            except ValueError: continue
            managed=registered.get(name)
            databases.append({'name':name,'managed':managed is not None,
                'managed_id':managed['id'] if managed else None})
        return {'databases':sorted(databases,key=lambda d:d['name'])}
    except Exception as exc:
        raise ValueError(safe_error(exc)) from None
    finally:
        if connection: connection.close()
