import json, os, time, urllib.request
BASE='http://127.0.0.1:8000'
def api(path,body=None):
 req=urllib.request.Request(BASE+'/api'+path,data=json.dumps(body).encode() if body is not None else None,headers={'Content-Type':'application/json'})
 with urllib.request.urlopen(req,timeout=60) as response: return json.load(response)
action=os.environ['VERIFY_ACTION']
if action=='start':
 c=api('/config')['config']
 c['connection']['password']='synthetic-verification-only'
 c['workload'].update(rps=20,workers=2,query_timeout=2,reconnect_timeout=2,complexity='LOW',simple_percent=100,join_percent=0,aggregate_percent=0,relational_insert_percent=0,relational_update_percent=0,transaction_percent=0)
 api('/config',c);del c['connection']['password']
 assert api('/connection/test',c['connection'])['success']
 api('/agent/start',{})
 time.sleep(2)
 print('STARTED',api('/status')['total'])
elif action=='down':
 time.sleep(3)
 s=api('/status')
 assert s['state']=='RECONNECTING',s['state']
 print(json.dumps({k:s.get(k) for k in ('state','total','failed','connection_errors')}))
elif action=='up':
 for _ in range(90):
  s=api('/status')
  if s['state']=='RUNNING' and s.get('reconnects',0)>0: break
  time.sleep(1)
 else: raise AssertionError('No recovery')
 time.sleep(2)
 final=api('/agent/stop',{})
 assert final['reconnects']>0 and final['success']>0
 print(json.dumps({'result':'RECOVERY PASS','reconnects':final['reconnects'],'failed':final['failed'],'successful':final['success'],'downtime_ms':final['failovers'][-1]['total_downtime_ms']}))
