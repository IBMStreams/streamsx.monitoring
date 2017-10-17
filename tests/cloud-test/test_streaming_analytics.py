import unittest

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester
import streamsx.spl.op as op
import streamsx.spl.toolkit as tk
import os
import streamsx.rest as sr

class TestCloud(unittest.TestCase):
    """ Test invocations of composite operators in Streaming Analytics Service """

    @classmethod
    def setUpClass(self):
        self.service_name = os.environ.get('STREAMING_ANALYTICS_SERVICE_NAME')
        # Get credentials from VCAP_SERVICES env, because values are required for the test_op as parameters
        self.credentials = sr._get_credentials(sr._get_vcap_services(), self.service_name)

    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=False)

    def _add_toolkits(self, topo):
        tk.add_toolkit(topo, './test_monitoring')
        tk.add_toolkit(topo, '../../com.ibm.streamsx.monitoring')

    def _get_expected_values(self):
        expected_vals = []
        expected_vals.append("TEST_RESULT_PASS")
        return expected_vals

    def _build_launch_validate(self, name, composite_name):
        topo = Topology(name)
        self._add_toolkits(topo)

        # Set up parameters to call the test composite
        user = self.credentials['userid']
        password = self.credentials['password']
        params = {'user':user, 'password':password}
	
        # Call the test composite
        test_op = op.Source(topo, composite_name, 'tuple<rstring result>', params=params)

        tester = Tester(topo)
        tester.tuple_count(test_op.stream, 1, exact=True)
        values = test_op.stream.map(lambda x: x['result'])
        tester.contents(values, self._get_expected_values())

        tester.test(self.test_ctxtype, self.test_config)

    def test_metrics_monitor(self):
        self._build_launch_validate("test_metrics_monitor", "test.metrics::TestMetricsSource")

    def test_logs_monitor(self):
        self._build_launch_validate("test_logs_monitor", "test.system::TestLogsSource")

    def test_jobs_status_monitor(self):
        self._build_launch_validate("test_jobs_status_monitor", "test.jobs::TestJobStatusSource")


