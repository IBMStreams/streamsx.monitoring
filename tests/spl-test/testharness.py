# Copyright (C) 2017 International Business Machines Corporation. 
# All Rights Reserved.

import sys, os, time
from subprocess import call, Popen, PIPE

class TestFailure(Exception):
    def __init__(self, out, err):
        self.stdout = out
        self.stderr = err

    def say(self, test_name):
        print (test_name + ' fail:\n' + '\tstdout: ' + self.stdout + '\n' + '\tstderr: ' + self.stderr + '\n')

def exec_noexit(seq):
    p = Popen(seq, stdout=PIPE, stderr=PIPE)
    stdout, stderr = p.communicate()
    return stdout, stderr, p.returncode

def assert_pass(condition, stdout, stderr):
    if not condition:
        raise TestFailure(stdout, stderr)

def start_sample(args=list()):
    stdout, stderr, err = exec_noexit(['make', 'start-sample'] + args)
    assert_pass(err == 0, stdout, stderr)

def stop_sample(args=list()):
    exec_noexit(['make', 'stop-sample'] + args)

def start_monitor(args=list()):
    stdout, stderr, err = exec_noexit(['make', 'start-monitor'] + args)
    assert_pass(err == 0, stdout, stderr)

def start_test_domain(args=list()):
    stdout, stderr, err = exec_noexit(['make', 'start-test-domain'] + args)
    assert_pass(err == 0, stdout, stderr)

def create_app_config(args=list()):
    stdout, stderr, err = exec_noexit(['make', 'configure'] + args)
    assert_pass(err == 0, stdout, stderr)

def stop_test_domain(args=list()):
    exec_noexit(['make', 'stop-test-domain'] + args)

def stop_monitor(args=list()):
    exec_noexit(['make', 'stop-monitor'] + args)

def make_applications():
    stdout, stderr, err = exec_noexit(['make', 'all'])
    assert_pass(err == 0, stdout, stderr)

def make_clean():
    stdout, stderr, err = exec_noexit(['make', 'clean'])
    assert_pass(err == 0, stdout, stderr)

def run_standalone(args=list()):
    return exec_noexit(['./output/bin/standalone'] + args)

def run_monitor_standalone(args=list()):
    return exec_noexit(['./output/monitor/bin/standalone'] + args)

def remove_f(name):
    '''Removes a file but ignores not existing files errors'''
    try:
        os.remove(name)
    except OSError as e:
        if e.errno != 2:
            raise

def remove_files(name):
    Popen(["rm -f " + name], shell=True, stdout=PIPE).communicate()

def wait_for_file(name):
    timeout = 180   # [seconds]
    timeout_start = time.time()
    while time.time() < timeout_start + timeout:
        if os.path.exists (name):
            break
        time.sleep(1)

def test_result_file(name):
    err = 0
    try:
        fh = open(name, 'r')
        result = fh.read()
        fh.close()
    except IOError:
        err = 1
    return err

def checkEnvJMX():
    try:
        os.environ["JMX_USER"]
        os.environ["JMX_PASSWORD"]
    except KeyError: 
        print ("ERROR: Please set the environment variables JMX_USER and JMX_PASSWORD")
        raise

def checkDomain():
    result = 0
    try:
        os.environ["TEST_DOMAIN"]
        os.environ["TEST_INSTANCE"]
    except KeyError: 
        result = 1
    return result

