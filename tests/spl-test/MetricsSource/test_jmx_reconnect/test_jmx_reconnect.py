import unittest
import os
import testharness as th

@unittest.skipIf(th.checkDomain() > 0, "Missing configuration for second Streams domain and instance. Environment variables TEST_DOMAIN and TEST_INSTANCE are not set.")
class MetricsSourceJmxReconnectTest(unittest.TestCase):

    result_file_1 = "done_1"
    result_file_2 = "done_2"

    @classmethod
    def setUpClass(cls):
        th.checkEnvJMX()

    def setUp(self):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        th.remove_f(self.result_file_1)
        th.remove_f(self.result_file_2)
        th.remove_files("StreamsLogsJob*.tgz")
        th.start_test_domain()

    def tearDown(self):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        th.remove_f(self.result_file_1)
        th.remove_f(self.result_file_2)
        th.stop_monitor()
        th.stop_sample()
        th.stop_test_domain()

    def test_with_two_domains(self):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))

        th.make_applications()
        th.start_monitor()
        th.start_sample()

        # test if monitor app receives notifications
        th.wait_for_file(self.result_file_1)
        err = th.test_result_file(self.result_file_1)
        self.assertEqual(err, 0)

        # force JMX connection failure and monitor app needs to reconnect
        th.stop_test_domain()
        th.start_test_domain()

        th.start_sample()

        # test if monitor app receives notifications
        th.wait_for_file(self.result_file_2)
        err = th.test_result_file(self.result_file_2)
        self.assertEqual(err, 0)

if __name__ == '__main__':
    unittest.main()

