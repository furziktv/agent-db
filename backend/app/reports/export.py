"""Complete exports of an append-only session at a fixed SQLite watermark."""
import asyncio
import csv
import io
import json
import os
import tempfile
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any
from fastapi import HTTPException
from app.monitoring.collector import utc
from app.reports.docx import DocxWriter

TABLES = ('samples', 'requests', 'events')
CSV_FIELDS = ['record_type','record_index','session_id','timestamp','worker_id','query_type',
              'query_class','template','tables','database_host','database_port','database_name',
              'execution_time_ms','connection_time_ms','payload_size','rows_read','rows_affected',
              'success','error_message','mysql_error_code','slow','data']
DOCX_GATE = asyncio.Semaphore(2)

def dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))

@dataclass
class Snapshot:
    store: Any
    metadata: dict
    watermarks: dict[str, int]

    async def records(self, table: str, errors_only: bool = False) -> AsyncIterator[dict]:
        if table not in TABLES:
            raise ValueError('Unknown export dataset')
        last = 0
        predicate = " AND json_extract(data,'$.success')=0" if errors_only else ''
        while True:
            rows = await self.store.fetch(
                f'SELECT rowid,data FROM {table} WHERE session_id=? AND rowid>? AND rowid<=?{predicate} ORDER BY rowid LIMIT 500',
                (self.metadata['session_id'],last,self.watermarks[table]),
            )
            if not rows:
                break
            for rowid, data in rows:
                last = rowid
                yield json.loads(data)

async def snapshot(store: Any, session_id: str) -> Snapshot:
    # One SQL statement observes session metadata and all bounds consistently,
    # while writers can keep appending after the snapshot is captured.
    expressions = ','.join(
        f'(SELECT COALESCE(MAX(rowid),0) FROM {table} WHERE session_id=s.id),'
        f'(SELECT COUNT(*) FROM {table} WHERE session_id=s.id)' for table in TABLES)
    rows = await store.fetch(
        f'SELECT s.id,s.started,s.ended,s.config,s.summary,{expressions},'
        "(SELECT COUNT(*) FROM requests WHERE session_id=s.id AND json_extract(data,'$.success')=0) "
        'FROM sessions s WHERE s.id=?', (session_id,),
    )
    if not rows:
        raise HTTPException(404,'Session not found')
    row = rows[0]
    counts = {table:row[6+i*2] for i,table in enumerate(TABLES)}
    counts['errors'] = row[-1]
    metadata = dict(session_id=row[0],start_time=row[1],end_time=row[2],
                    config=json.loads(row[3]),summary=json.loads(row[4]) if row[4] else {},
                    export=dict(exported_at=utc(),scope='All persisted session records at snapshot time',
                                active_session=row[2] is None,counts=counts))
    return Snapshot(store, metadata, {table:row[5+i*2] for i,table in enumerate(TABLES)})

async def json_stream(data: Snapshot) -> AsyncIterator[str]:
    yield dumps(data.metadata)[:-1]
    for name, table, errors in [('history','samples',False),('requests','requests',False),
                                ('errors','requests',True),('events','events',False)]:
        yield ',"'+name+'":['
        first = True
        batch = []
        async for record in data.records(table,errors):
            batch.append(('' if first else ',')+dumps(record))
            first = False
            if len(batch) == 100:
                yield ''.join(batch)
                batch.clear()
        if batch:
            yield ''.join(batch)
        yield ']'
    yield '}'

async def csv_stream(data: Snapshot) -> AsyncIterator[str]:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer,fieldnames=CSV_FIELDS,extrasaction='ignore',lineterminator='\r\n')
    writer.writeheader()
    yield '\ufeff'+buffer.getvalue()
    def encode(kind: str, index: int, record: Any) -> str:
        buffer.seek(0)
        buffer.truncate(0)
        fields = record if isinstance(record,dict) else {}
        row = {key:fields.get(key,'') for key in CSV_FIELDS}
        for key,value in row.items():
            if isinstance(value,(dict,list)):
                row[key] = dumps(value)
        row.update(record_type=kind,record_index=index,session_id=data.metadata['session_id'],data=dumps(record))
        writer.writerow(row)
        return buffer.getvalue()
    for key,value in data.metadata.items():
        yield encode(key,1,value)
    for kind,table in [('history','samples'),('request','requests'),('event','events')]:
        index = 0
        async for record in data.records(table):
            index += 1
            yield encode(kind,index,record)

async def docx_file(data: Snapshot) -> str:
    async with DOCX_GATE:
        fd,path = tempfile.mkstemp(suffix='.docx',prefix='mysql-agent-report-')
        os.close(fd)
        writer = None
        try:
            writer = DocxWriter(path,data.metadata)
            async def append(records: list[dict]) -> None:
                task = asyncio.create_task(asyncio.to_thread(writer.records, records))
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    await task
                    raise
            for title,table in [('История показателей','samples'),('Все операции','requests'),('Системные события','events')]:
                writer.section(title)
                batch = []
                total = 0
                async for record in data.records(table):
                    batch.append(record)
                    total += 1
                    if len(batch) == 100:
                        await append(batch)
                        batch = []
                if batch:
                    await append(batch)
                if not total:
                    writer.paragraph('Записей нет')
            writer.close()
            return path
        except BaseException:
            if writer:
                writer.abort()
            os.unlink(path)
            raise
