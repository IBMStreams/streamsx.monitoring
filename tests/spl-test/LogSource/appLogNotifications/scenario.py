import testharness as th
import os, sys

try:
   os.environ["STREAMS_DOMAIN_ID"]
   os.environ["JMX_USER"]
   os.environ["JMX_PASSWORD"]
except KeyError: 
   print ("Please set the environment variables STREAMS_DOMAIN_ID, JMX_USER and JMX_PASSWORD")
   sys.exit(1)

def test():
    stdout, stderr, err = th.make_all()
    th.assert_pass(err == 0, stdout, stderr)

    th.start_sample()

    stdout, stderr, err = th.run_monitor_standalone(args=['user='+os.environ["JMX_USER"], 'password='+os.environ["JMX_PASSWORD"], 'domainId='+os.environ["STREAMS_DOMAIN_ID"]])

    #print 'Monitor:\n' + stdout + '\n'

    th.stop_sample()

    th.assert_pass(err == 0 and (stdout.find("TEST_RESULT_PASS") != -1), 
                   stdout, stderr)

