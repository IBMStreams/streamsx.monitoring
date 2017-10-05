import unittest
import os
import testharness as th

class LogSourceNotificationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        try:
           os.environ["JMX_USER"]
           os.environ["JMX_PASSWORD"]
        except KeyError: 
           print ("ERROR: Please set the environment variables JMX_USER and JMX_PASSWORD")
           raise

    def tearDown(self):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        th.stop_sample()

    def test_standalone(self):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        th.make_applications()
        th.start_sample()
        stdout, stderr, err = th.run_monitor_standalone(args=['user='+os.environ["JMX_USER"], 'password='+os.environ["JMX_PASSWORD"], 'domainId='+os.environ["STREAMS_DOMAIN_ID"]])
        th.assert_pass(err == 0 and (str(stdout).find('TEST_RESULT_PASS') != -1), stdout, stderr)

if __name__ == '__main__':
    unittest.main()

