
#Imports
from streamsx.topology.topology import *
from streamsx.topology.context import *
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.topology import context
from streamsx.spl.toolkit import add_toolkit
import streamsx.spl.op as op


monitoring_toolkit = '../com.ibm.streamsx.monitoring'
sample_toolkit = 'com.ibm.streamsx.monitoring.system.sample.SystemMonitorSource'

def _launch(main):
    cfg = {}
    cfg[context.ConfigParams.SSL_VERIFY] = False
    rc = context.submit('DISTRIBUTED', main, cfg)

def sample_app():
    topo = Topology('SystemMonitorSample')
    add_toolkit(topo, monitoring_toolkit)
    add_toolkit(topo, sample_toolkit)
    r = op.main_composite(kind='com.ibm.streamsx.monitoring.system.sample.SystemMonitorSource::Monitor', toolkits=[monitoring_toolkit,sample_toolkit])
    _launch(r[0])


sample_app()



