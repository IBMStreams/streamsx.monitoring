
#Imports
from streamsx.topology.topology import *
from streamsx.topology.context import *
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.topology import context
from streamsx.spl.toolkit import add_toolkit
import streamsx.spl.op as op


monitoring_toolkit = '../com.ibm.streamsx.monitoring'

def _launch(main):
    cfg = {}
    job_config = context.JobConfig(tracing='info')
    job_config.add(cfg)
    cfg[context.ConfigParams.SSL_VERIFY] = False
    rc = context.submit('DISTRIBUTED', main, cfg)

def metrics_ingest_service():
    topo = Topology('MetricsIngestService')
    add_toolkit(topo, monitoring_toolkit)
    r = op.main_composite(kind='com.ibm.streamsx.monitoring.metrics.services::MetricsIngestService', toolkits=[monitoring_toolkit])
    _launch(r[0])

def job_status_service():
    topo = Topology('JobStatusService')
    add_toolkit(topo, monitoring_toolkit)
    r = op.main_composite(kind='com.ibm.streamsx.monitoring.jobs.services::JobStatusService', toolkits=[monitoring_toolkit])
    _launch(r[0])

def failed_pe_service():
    topo = Topology('FailedPEService')
    add_toolkit(topo, monitoring_toolkit)
    r = op.main_composite(kind='com.ibm.streamsx.monitoring.jobs.services::FailedPEService', toolkits=[monitoring_toolkit])
    _launch(r[0])


metrics_ingest_service()
job_status_service()
failed_pe_service()


topo = Topology("SubscribeSample")
jobs_status_schema = StreamSchema('tuple<rstring notifyType,rstring instanceId,int64 jobId,rstring jobName,rstring resource,int64 peId,rstring peHealth,rstring peStatus,timestamp eventTimestamp>')
ts = topo.subscribe('streamsx/monitoring/jobs/status', schema=jobs_status_schema)
ts.print()
ts.isolate()

s = topo.subscribe('streamsx/monitoring/metrics/values', schema=CommonSchema.Json)
s.print()
s.isolate()

_launch(topo)


