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

    def _get_iam_endpoint(self):
        iamTokenEndpoint = "" # uses operator default
        # check if running on test system
        v2_rest_url = self.credentials.get("v2_rest_url")
        if "stage1" in v2_rest_url:
            iamTokenEndpoint = "https://iam.stage1.ng.bluemix.net/oidc/token"
        return iamTokenEndpoint

    def _build_launch_validate(self, name, composite_name):
        topo = Topology(name)
        self._add_toolkits(topo)

        # Set up parameters to call the test composite
        user = self.credentials.get("userid")
        password = self.credentials.get("password")
        
        if user is not None:
            print("Monitor application in the Streaming Analytics service uses user and password")
            params = {'user':user, 'password':password}
        else :
            print("Monitor application in the Streaming Analytics service uses IAM API KEY")
            iamApiKey = self.credentials.get("apikey") # use value from VCAP_SERVICES file
            iamTokenEndpoint = self._get_iam_endpoint()
            params = {'iamApiKey':iamApiKey, 'iamTokenEndpoint':iamTokenEndpoint}
	
        # Call the test composite
        test_op = op.Source(topo, composite_name, 'tuple<rstring result>', params=params)

        tester = Tester(topo)
        tester.tuple_count(test_op.stream, 1, exact=True)
        tester.contents(test_op.stream, [{'result':'TEST_RESULT_PASS'}] )

        tester.test(self.test_ctxtype, self.test_config)


    def test_metrics_monitor(self):
        self._build_launch_validate("test_metrics_monitor", "test.metrics::TestMetricsSource")

    def test_logs_monitor(self):
        self._build_launch_validate("test_logs_monitor", "test.system::TestLogsSource")

    def test_jobs_status_monitor(self):
        self._build_launch_validate("test_jobs_status_monitor", "test.jobs::TestJobStatusSource")


