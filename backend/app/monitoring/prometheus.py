from app.monitoring.collector import sql_activity
from prometheus_client import CollectorRegistry, Counter, Histogram, Gauge, generate_latest

class Prometheus:
    def __init__(self):
        self.registry=CollectorRegistry()
        labels=('host','query_type')
        self.requests=Counter('mysql_agent_requests_total','Completed workload operations',labels,registry=self.registry)
        self.success=Counter('mysql_agent_requests_success_total','Successful operations',labels,registry=self.registry)
        self.failed=Counter('mysql_agent_requests_failed_total','Failed operations',labels,registry=self.registry)
        self.duration=Histogram('mysql_agent_request_duration_seconds','SQL execution time',labels,registry=self.registry)
        self.operations={op:Counter('mysql_agent_'+op.lower()+'_total',op+' operations',('host',),registry=self.registry) for op in ('SELECT','INSERT','UPDATE')}
        self.connection_errors=Counter('mysql_agent_connection_errors_total','Connection errors',registry=self.registry)
        self.integrity_checks=Counter('mysql_agent_integrity_checks_total','Completed write verification reads',registry=self.registry)
        self.integrity_lost=Counter('mysql_agent_integrity_lost_writes_total','Missing or mismatched write verification reads',registry=self.registry)
        self.integrity_inconclusive=Counter('mysql_agent_integrity_inconclusive_total','Uncompleted verification reads due to SQL or transport errors',registry=self.registry)
        self.refreshes=Counter('mysql_agent_connection_refreshes_total','Planned connection refreshes',registry=self.registry)
        self.reconnects=Counter('mysql_agent_reconnects_total','Recoveries',registry=self.registry)
        self.bytes=Counter('mysql_agent_payload_bytes_total','Generated payload bytes',registry=self.registry)
        self.read=Counter('mysql_agent_rows_read_total','Rows read',registry=self.registry)
        self.written=Counter('mysql_agent_rows_written_total','Rows written',registry=self.registry)
        self.connections=Gauge('mysql_agent_connections','Connections currently executing',registry=self.registry)
        self.running=Gauge('mysql_agent_running','Running state',registry=self.registry)
        # Session ID is exposed only as one replaceable info gauge, not on cumulative series.
        self.session=Gauge('mysql_agent_session_info','Current session',('session_id','host'),registry=self.registry)
    def start(self,id,host):
        self.session.clear()
        self.session.labels(id,host).set(1)
    def record(self,e):
        labels=(e['database_host'],e['query_type'])
        self.requests.labels(*labels).inc()
        (self.success if e['success'] else self.failed).labels(*labels).inc()
        self.duration.labels(*labels).observe(e['execution_time_ms']/1000)
        for operation,count in sql_activity(e).items():
            self.operations[operation].labels(e['database_host']).inc(count)
        if e.get('connection_refreshed'): self.refreshes.inc()
        if e.get('error_kind')=='connection_errors': self.connection_errors.inc()
        self.integrity_checks.inc(e.get('integrity_checks',0))
        self.integrity_lost.inc(e.get('integrity_lost_writes',0))
        self.integrity_inconclusive.inc(e.get('integrity_inconclusive',0))
        self.bytes.inc(e.get('bytes_generated',0))
        self.read.inc(e.get('rows_read',0))
        self.written.inc(e.get('rows_affected',0))
    def render(self,agent):
        self.connections.set(agent.active_connections)
        self.running.set(int(agent.state=='RUNNING'))
        return generate_latest(self.registry)
