import pytest
from fastapi.testclient import TestClient
from app.main import app

@pytest.fixture
def client(tmp_path,monkeypatch):
    monkeypatch.setenv('SQLITE_PATH',str(tmp_path/'api.sqlite3'))
    with TestClient(app) as c: yield c

def test_start_requires_verified_connection(client):
    assert client.post('/api/agent/start').status_code==400
    assert client.get('/health').status_code==200
    assert client.get('/ready').status_code==200
    assert client.get('/metrics').status_code==200

def test_validation_never_echoes_password(client):
    data=client.get('/api/config').json()['config']
    data['connection']['password']='VERY_SECRET_PASSWORD'
    data['connection']['port']=999999
    response=client.post('/api/config',json=data)
    assert response.status_code==422
    assert 'VERY_SECRET_PASSWORD' not in response.text

def test_same_origin_and_password_redaction(client):
    data=client.get('/api/config').json()['config']
    data['connection']['password']='VERY_SECRET_PASSWORD'
    assert client.post('/api/config',json=data,headers={'Origin':'https://evil.invalid'}).status_code==403
    assert client.post('/api/config',json=data).status_code==200
    assert 'VERY_SECRET_PASSWORD' not in client.get('/api/config').text
    assert 'VERY_SECRET_PASSWORD' not in client.get('/api/status').text
    with client.websocket_connect('/ws/live') as ws:
        message=ws.receive_json()
        assert 'VERY_SECRET_PASSWORD' not in str(message)

def test_reports_and_management_are_guarded(client):
    assert client.get('/api/reports').json()==[]
    assert client.get('/api/reports/missing').status_code==404
    assert client.post('/api/databases/create',json={'name':'workload_test_demo','confirmation':'workload_test_demo'}).status_code==400
    assert client.post('/api/databases/missing/delete',json={'confirmation':'production'}).status_code==400

def test_database_picker_requires_verified_connection(client):
    assert client.get('/api/connection/databases').status_code==400

def test_database_picker_filters_system_schemas_and_marks_managed(client,monkeypatch):
    from unittest.mock import AsyncMock,Mock
    from app.models.config import Config,ConnectionConfig
    from app.main import app
    agent=app.state.agent
    agent.config=Config(connection=ConnectionConfig(host='test.example'))
    agent.verified=agent.fingerprint()
    cursor=AsyncMock()
    cursor.__aenter__.return_value=cursor
    cursor.fetchall.return_value=[('mysql',),('performance_schema',),('sys',),('information_schema',),('agent_test',),('workload_test_picker',),('invalid-name',)]
    conn=Mock()
    conn.cursor.return_value=cursor
    # Endpoint imports the connector at call time.
    from app.database import connection
    monkeypatch.setattr(connection,'connect',AsyncMock(return_value=conn))
    monkeypatch.setattr(agent.store,'databases',AsyncMock(return_value=[{'id':'owned','name':'workload_test_picker','host':'test.example','port':3306,'status':'READY'},{'id':'other','name':'agent_test','host':'other.example','port':3306,'status':'READY'}]))
    response=client.get('/api/connection/databases')
    assert response.status_code==200
    assert response.json()['databases']==[
        {'name':'agent_test','managed':False,'managed_id':None},
        {'name':'workload_test_picker','managed':True,'managed_id':'owned'},
    ]
    conn.close.assert_called_once()

def test_test_settings_are_persisted_and_server_validated(client):
    data=client.get('/api/config').json()['config']
    data['connection']['database']='selected_test_database'
    data['workload'].update(workers=3,rps=25,duration_seconds=60,max_requests=1000,connection_refresh_every=5)
    assert client.post('/api/config',json=data).status_code==200
    saved=client.get('/api/config').json()['config']
    assert saved['connection']['database']=='selected_test_database'
    assert saved['workload']['duration_seconds']==60 and saved['workload']['max_requests']==1000
    data['workload']['duration_seconds']=-1
    assert client.post('/api/config',json=data).status_code==422

def test_complete_report_export_routes_and_docx_download(client):
    import io,json,os,sqlite3
    from zipfile import ZipFile
    with sqlite3.connect(os.environ['SQLITE_PATH']) as db:
        db.execute('INSERT INTO sessions VALUES (?,?,?,?,?)',('route-session','2026-10-05','2026-10-05','{"workload":{"rps":10}}','{"total":1}'))
        db.execute('INSERT INTO requests VALUES (?,?,?)',('route-session','2026-10-05',json.dumps({'timestamp':'2026-10-05','success':False,'extra':{'new_field':1}})))
    result=client.get('/api/reports/route-session/export?format=json')
    assert result.status_code==200
    assert result.json()['requests'][0]['extra']=={'new_field':1}
    assert len(result.json()['errors'])==1
    result=client.get('/api/reports/route-session/export?format=csv')
    assert result.status_code==200 and 'new_field' in result.text
    result=client.get('/api/reports/route-session/export?format=docx')
    assert result.status_code==200
    assert result.headers['content-type']=='application/vnd.openxmlformats-officedocument.wordprocessingml.document'
    assert 'route-session.docx' in result.headers['content-disposition']
    with ZipFile(io.BytesIO(result.content)) as package:
        assert package.testzip() is None
        assert b'new_field' in package.read('word/document.xml')
    assert client.get('/api/reports/missing/export?format=docx').status_code==404
    assert client.get('/api/reports/route-session/export?format=invalid').status_code==400
