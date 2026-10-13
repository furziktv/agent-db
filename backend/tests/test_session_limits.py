import time
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from app.agent.manager import AgentManager
from app.models.config import Config,WorkloadConfig

@pytest.mark.asyncio
@pytest.mark.parametrize('duration,requests,elapsed,dispatched',[(1,0,2,0),(0,5,0,5)])
async def test_automatic_stop_saves_report(duration,requests,elapsed,dispatched):
    store=SimpleNamespace(error=None,execute=AsyncMock(),queue=SimpleNamespace(join=AsyncMock()))
    agent=AgentManager(store)
    agent.config=Config(workload=WorkloadConfig(duration_seconds=duration,max_requests=requests))
    agent.state='RUNNING'
    agent.session_id='bounded-session'
    agent.dispatched=dispatched
    agent.collector.started=time.monotonic()-elapsed
    agent.database_manager=SimpleNamespace(progress={'state':'IDLE'})
    agent.nodes=SimpleNamespace(public=lambda:[])
    result=await agent.tick()
    assert result['metrics']['state']=='STOPPED'
    assert agent.stop_event.is_set()
    assert agent.ended_at and agent.final_summary['session_id']=='bounded-session'
    store.execute.assert_awaited()
    assert store.execute.call_args.args[0].startswith('UPDATE sessions SET ended=')
