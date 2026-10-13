"""Live API verification against an explicitly configured disposable MySQL server.
Run with VERIFY_MYSQL_HOST, VERIFY_MYSQL_USER and VERIFY_MYSQL_PASSWORD.
Never point this script at a production database.
"""
import asyncio
import json
import os
import time
import urllib.request
import urllib.error

BASE=os.getenv('AGENT_URL','http://127.0.0.1:8000')
DB=os.getenv('VERIFY_DATABASE','workload_test_verification')
def api(path,body=None):
    request=urllib.request.Request(BASE+'/api'+path,data=json.dumps(body).encode() if body is not None else None,headers={'Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(request,timeout=180) as response: return json.load(response)
    except urllib.error.HTTPError as exc:
        raise AssertionError(f'{path}: {exc.code}: {exc.read().decode()}') from None

def wait_generator():
    for _ in range(180):
        p=api('/databases/progress')
        if p['state']=='READY': return p
        assert p['state'] not in ('FAILED','CANCELLED'),p
        time.sleep(1)
    raise AssertionError('Generator deadline exceeded')

def main():
    c=api('/config')['config']
    c['connection'].update(host=os.environ['VERIFY_MYSQL_HOST'],username=os.environ['VERIFY_MYSQL_USER'],password=os.environ['VERIFY_MYSQL_PASSWORD'],database=DB)
    c['workload'].update(workers=4,rps=50,min_payload=50,max_payload=200,pool_max_size=4,max_rows=10)
    api('/config',c)
    del c['connection']['password']
    info=api('/connection/test',c['connection'])
    assert info['success'],info
    req=dict(name=DB,confirmation=DB,records=1500,payload_size=80,batch_size=100)
    if not os.getenv('VERIFY_EXISTING'):
        api('/databases/create',req)
        wait_generator()
    db=next(d for d in api('/databases') if d['name']==DB and d['status']=='READY')
    assert db['rows']>=1500 and len(db['tables'])==15,db
    api('/databases/'+db['id']+'/use',{})
    api('/agent/start',{})
    time.sleep(12)
    api('/agent/pause',{})
    time.sleep(.5)
    paused=api('/status')['total']
    time.sleep(1.5)
    assert api('/status')['total']==paused,'Pause must stop dispatching'
    api('/agent/resume',{})
    time.sleep(4)
    final=api('/agent/stop',{})
    assert final['total']>=600,final
    assert final['failed']==0,{k:final.get(k) for k in ('total','failed','sql_errors','connection_errors')}
    assert final['table_coverage']['used']==15,final
    assert final['successful_transactions']>0,final
    report=api('/reports/'+final['session_id'])
    assert report['end_time'] and report['history']
    with urllib.request.urlopen(BASE+'/api/reports/'+final['session_id']+'/export?format=csv') as response:
        lines=response.read().decode().splitlines()
    assert len(lines)==final['total']+1
    api('/databases/'+db['id']+'/add',dict(req,records=300))
    wait_generator()
    updated=next(d for d in api('/databases') if d['id']==db['id'])
    assert updated['rows']>=1800,updated
    print(json.dumps({'result':'PASS','queries':final['total'],'failed':final['failed'],'average_rps':final['average_rps'],
      'coverage':final['table_coverage'],'transactions':final['successful_transactions'],'rows_after_append':updated['rows'],'session':final['session_id']}))

if __name__=='__main__': main()
