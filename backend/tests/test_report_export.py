import asyncio
import csv
import io
import json
import os
import xml.etree.ElementTree as ET
from zipfile import ZipFile
import pytest
from app.reports.manager import Store
from app.reports.export import snapshot,json_stream,csv_stream,docx_file

@pytest.fixture
async def dataset(tmp_path,monkeypatch):
    monkeypatch.setenv('SQLITE_PATH',str(tmp_path/'reports.sqlite3'))
    store=Store()
    await store.open()
    await store.execute('INSERT INTO sessions VALUES (?,?,?,?,?)',('export-session','start','end',json.dumps({'connection':{'host':'example.test'},'workload':{'rps':100}}),json.dumps({'total':1205,'failed':305})))
    async with store.lock:
        for table,count in [('requests',1205),('samples',4001),('events',207)]:
            await store.db.executemany(f'INSERT INTO {table} VALUES (?,?,?)',[
                ('export-session',str(i),json.dumps({'timestamp':str(i),'success':i>=305,'index':i,'extra':{'nested':['тест','special<&\u0001']},'query_type':'SELECT','tables':['users'],'bytes_generated':i})) for i in range(count)])
        await store.db.commit()
    yield store
    await store.close()

@pytest.mark.asyncio
async def test_json_has_all_records_and_freezes_append_only_snapshot(dataset):
    snap=await snapshot(dataset,'export-session')
    await dataset.execute('INSERT INTO requests VALUES (?,?,?)',('export-session','later','{"success":false,"later":true}'))
    result=json.loads(''.join([chunk async for chunk in json_stream(snap)]))
    assert len(result['requests'])==1205
    assert len(result['history'])==4001
    assert len(result['errors'])==305
    assert len(result['events'])==207
    assert result['requests'][-1]['index']==1204
    assert result['requests'][1]['extra']['nested'][0]=='тест'
    assert result['export']['counts']=={'requests':1205,'samples':4001,'events':207,'errors':305}
    assert not any('later' in r for r in result['requests'])

@pytest.mark.asyncio
async def test_csv_includes_metadata_all_datasets_and_unknown_fields(dataset):
    result=''.join([chunk async for chunk in csv_stream(await snapshot(dataset,'export-session'))])
    rows=list(csv.DictReader(io.StringIO(result.lstrip('\ufeff'))))
    requests=[r for r in rows if r['record_type']=='request']
    assert len(requests)==1205
    assert len([r for r in rows if r['record_type']=='history'])==4001
    assert len([r for r in rows if r['record_type']=='event'])==207
    assert json.loads(requests[-1]['data'])['bytes_generated']==1204
    assert json.loads(requests[0]['data'])['extra']['nested'][1]=='special<&\u0001'
    assert any(r['record_type']=='config' for r in rows)
    assert any(r['record_type']=='summary' for r in rows)

@pytest.mark.asyncio
async def test_docx_includes_every_record_with_valid_ooxml(dataset):
    path=await docx_file(await snapshot(dataset,'export-session'))
    try:
        with ZipFile(path) as package:
            assert package.testzip() is None
            root=ET.fromstring(package.read('word/document.xml'))
            values=[node.text for node in root.iter('{http://schemas.openxmlformats.org/wordprocessingml/2006/main}t') if node.text]
            records=[json.loads(value) for value in values if value.startswith('{"timestamp"')]
            assert len(records)==1205+4001+207
            assert sum(r['index']==1204 for r in records)==2
            assert any(r['index']==4000 for r in records)
            assert records[0]['extra']['nested'][1]=='special<&\u0001'
    finally:
        os.unlink(path)
