import testharness as th
import os, sys

try:
   os.environ["STREAMS_DOMAIN_ID"]
   os.environ["JMX_USER"]
   os.environ["JMX_PASSWORD"]
   os.environ["TEST_DOMAIN"]
   os.environ["TEST_INSTANCE"]
except KeyError: 
   print ("Please set the environment variables TEST_DOMAIN, TEST_INSTANCE, STREAMS_DOMAIN_ID, JMX_USER and JMX_PASSWORD")
   sys.exit(1)

def test():
    result_file_1 = "done_1"; 
    result_file_2 = "done_2";
    th.remove_f(result_file_1)
    th.remove_f(result_file_2)
    th.remove_files("StreamsLogsJob*.tgz")

    th.start_test_domain()

    stdout, stderr, err = th.make_all()
    th.assert_pass(err == 0, stdout, stderr)

    th.start_monitor()
    th.start_sample()

    th.wait_for_file(result_file_1)    
    err = 0
    try:
        result = open(result_file_1, 'r').read()
    except IOError:
        err = 1
        th.stop_monitor()
        th.stop_sample()
        th.stop_test_domain()
        th.assert_pass(err == 0, "Could not read file:", result_file_1)

    th.stop_test_domain()
    th.start_test_domain()
    th.start_sample()

    th.wait_for_file(result_file_2)    
    err = 0
    try:
        result = open(result_file_2, 'r').read()
    except IOError:
        err = 1
        th.stop_monitor()
        th.stop_sample()
        th.stop_test_domain()
        th.assert_pass(err == 0, "Could not read file:", result_file_2)


    th.remove_f(result_file_1)
    th.remove_f(result_file_2)
    th.stop_monitor()
    th.stop_sample()
    th.stop_test_domain()

