import time
import random
from collections import Counter, deque
from datetime import datetime, timezone

def utc() -> str:
    return datetime.now(timezone.utc).isoformat()

def percentiles(values) -> dict:
    a = sorted(values)
    def p(q):
        if not a: return 0.0
        pos = (len(a)-1)*q
        lo = int(pos)
        return a[lo] + (a[min(lo+1,len(a)-1)]-a[lo])*(pos-lo)
    return {'p50':p(.50),'p95':p(.95),'p99':p(.99)}

def sql_activity(event: dict) -> dict[str, int]:
    counts = event.get('sql_counts')
    if counts is not None:
        return {op:counts.get(op,0) for op in ('SELECT','INSERT','UPDATE')}
    # Older events contain only the enclosing workload operation.
    return {op:int(event.get('success',False) and event['query_type']==op)
            for op in ('SELECT','INSERT','UPDATE')}

class Collector:
    def __init__(self):
        self.started = time.monotonic()
        self.counts = Counter()
        self.latencies = []
        self.latency_sum = 0.0
        self.minimum = None
        self.maximum = 0.0
        self.tables = {}
        self.templates = {}
        self.recent = deque()
        self.logs = deque(maxlen=1000)
        self.log_seq = 0
        self.peak_rps = 0.0
        self.last_used = {}
        self.type_samples = {}
        self.failovers = []
        self.slow = deque(maxlen=100)
        self.sample_rng = random.Random()

    def log(self, event):
        self.log_seq += 1
        event = dict(event, seq=self.log_seq)
        self.logs.append(event)

    def record(self, event):
        c = self.counts
        c['total'] += 1
        c['success' if event['success'] else 'failed'] += 1
        op, kind = event['query_type'], event['query_class']
        c[op] += 1
        c[kind] += 1
        activity = sql_activity(event)
        for operation,count in activity.items():
            c['sql_'+operation.lower()+'_total'] += count
        c['connection_refreshes'] += int(event.get('connection_refreshed',False))
        for key in ('rows_read','rows_inserted','rows_updated','bytes_generated','bytes_read','statements','integrity_checks','integrity_lost_writes','integrity_missing','integrity_mismatches','integrity_inconclusive'):
            c[key] += event.get(key,0)
        if event.get('error_kind'):
            c[event['error_kind']] += 1
        if kind == 'transaction':
            c['successful_transactions' if event['success'] else 'rolled_back_transactions'] += 1
        latency = event['execution_time_ms']
        self.latency_sum += latency
        self.minimum = latency if self.minimum is None else min(self.minimum,latency)
        self.maximum = max(self.maximum,latency)
        if len(self.latencies) < 20000:
            self.latencies.append(latency)
        else:
            j = self.sample_rng.randrange(c['total'])
            if j < len(self.latencies): self.latencies[j] = latency
        samples = self.type_samples.setdefault(op, deque(maxlen=5000))
        samples.append(latency)
        now = time.monotonic()
        self.recent.append((now, op, kind, event['success'], latency, event.get('bytes_read',0)+event.get('bytes_generated',0),event.get('integrity_lost_writes',0),event.get('statements',0),event.get('integrity_checks',0),activity))
        for table in event['tables']:
            self.last_used[table] = now
            t = self.tables.setdefault(table, Counter())
            details=event.get('table_stats',{}).get(table)
            if details:
                t.update(details)
            else:
                t[op] += 1
                t[op+'_latency_sum'] += latency
            t['errors'] += int(not event['success'])
        t = self.templates.setdefault(event['template'], Counter())
        t['count'] += 1
        t['latency_sum'] += latency
        t['max_latency'] = max(t['max_latency'],latency)
        if event.get('slow'): self.slow.append(event)
        self.log(event)

    def snapshot(self, expected_tables=()):
        now = time.monotonic()
        while self.recent and self.recent[0][0] < now - 1:
            self.recent.popleft()
        duration = now-self.started
        window = max(.001,min(1.0,duration))
        rps = len(self.recent)/window
        self.peak_rps = max(self.peak_rps,rps)
        c = self.counts
        return dict(c, total=c['total'], success=c['success'], failed=c['failed'],
            duration=duration, uptime=duration, rps=rps, peak_rps=self.peak_rps,
            average_rps=c['total']/max(duration,.001), avg_latency=self.latency_sum/max(c['total'],1),
            min_latency=self.minimum or 0, max_latency=self.maximum, **percentiles(self.latencies),
            percentile_method='reservoir sample, max 20000; type percentiles: last 5000',
            select_rps=sum(r[9]['SELECT'] for r in self.recent)/window,
            insert_rps=sum(r[9]['INSERT'] for r in self.recent)/window,
            update_rps=sum(r[9]['UPDATE'] for r in self.recent)/window,
            transaction_rps=sum(r[2]=='transaction' for r in self.recent)/window,
            join_rps=sum(r[2]=='join' for r in self.recent)/window,
            errors_rps=sum(not r[3] for r in self.recent)/window,
            integrity_lost_writes=c['integrity_lost_writes'], integrity_checks=c['integrity_checks'],
            integrity_inconclusive=c['integrity_inconclusive'],
            integrity_loss_rps=sum(r[6] for r in self.recent)/window,
            integrity_checks_rps=sum(r[8] for r in self.recent)/window,
            sql_rps=sum(r[7] for r in self.recent)/window,
            traffic_bps=sum(r[5] for r in self.recent)/window,
            tables={k:dict(v, **{op+'_avg_latency':v[op+'_latency_sum']/max(v[op],1) for op in ('SELECT','INSERT','UPDATE')}) for k,v in self.tables.items()},
            templates={k:dict(v,avg_latency=v['latency_sum']/max(v['count'],1)) for k,v in self.templates.items()},
            latency_by_type={k:percentiles(v) for k,v in self.type_samples.items()},
            table_coverage=dict(used=sum(k in self.tables for k in expected_tables),total=len(expected_tables)),
            failovers=self.failovers, slow_queries=list(self.slow))
