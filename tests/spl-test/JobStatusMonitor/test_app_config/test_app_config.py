import unittest
import os
import testharness as th

class JobStatusMonitorAppConfigTest(unittest.TestCase):

    result_file_1 = "done_1"

    @classmethod
    def setUpClass(cls):
        try:
           os.environ["JMX_USER"]
           os.environ["JMX_PASSWORD"]
        except KeyError: 
           print ("ERROR: Please set the environment variables JMX_USER and JMX_PASSWORD")
           raise

    def setUp(self):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        th.remove_f(self.result_file_1)
        th.remove_files("StreamsLogsJob*.tgz")

    def tearDown(self):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        th.remove_f(self.result_file_1)
        th.stop_monitor()
        th.stop_sample()
        th.rm_app_config()

    def test_distributed(self):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))

        th.create_app_config()
        th.make_applications()
        th.start_monitor()
        th.start_sample()
        th.wait_for_file(self.result_file_1)
        err = th.test_result_file(self.result_file_1)
        self.assertEqual(err, 0)

if __name__ == '__main__':
    unittest.main()

