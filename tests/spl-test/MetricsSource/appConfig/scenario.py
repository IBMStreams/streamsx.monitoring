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
    result_file_1 = "done_1"; 
    th.remove_f(result_file_1)
    th.remove_files("StreamsLogsJob*.tgz")

    th.create_app_config()

    stdout, stderr, err = th.make_all()
    th.assert_pass(err == 0, stdout, stderr)

    th.start_monitor()

    th.wait_for_file(result_file_1)
    err = 0
    try:
        result = open(result_file_1, 'r').read()
    except IOError:
        err = 1
        th.stop_monitor()
        assert_pass(err == 0, "Could not read file:", result_file_1)

    th.remove_f(result_file_1)
    th.stop_monitor()

