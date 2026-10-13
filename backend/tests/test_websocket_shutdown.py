import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pytest
from app.main import app, live

@pytest.mark.asyncio
async def test_websocket_handler_finishes_when_client_disconnects(monkeypatch):
    monkeypatch.setattr(app.state,'live',{'metrics':{'session_id':None},'logs':[]},raising=False)
    websocket=SimpleNamespace(headers={},accept=AsyncMock(),
        receive=AsyncMock(return_value={'type':'websocket.disconnect','code':1012}),
        send_json=AsyncMock())
    await asyncio.wait_for(live(websocket),timeout=.5)
    websocket.receive.assert_awaited_once()
