# Copyright (C) 2015 International Business Machines Corporation. 
# All Rights Reserved.

import sys, os
from subprocess import call, Popen, PIPE

class TestFailure(Exception):
    def __init__(self, out, err):
        self.stdout = out
        self.stderr = err

    def say(self, test_name):
        print test_name + ' fail:\n' + \
              '\tstdout: ' + self.stdout + '\n' + \
              '\tstderr: ' + self.stderr + '\n'

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

def make_build(args=list()):
    return exec_noexit(['make', 'build'] + args)

def make_all(args=list()):
    return exec_noexit(['make', 'all'] + args)

def make_clean():
    stdout, stderr, err = exec_noexit(['make', 'clean'])
    assert_pass(err == 0, stdout, stderr)

def run_standalone(args=list()):
    return exec_noexit(['./output/bin/standalone'] + args)

def run_monitor_standalone(args=list()):
    return exec_noexit(['./output/monitor/bin/standalone'] + args)


def testharness(test_name):
    topdir = os.getcwd()
    try:
        os.chdir(test_name)

        sys.path.append(os.getcwd())
        tester = __import__('scenario')

        tester.test()
        make_clean()

        del tester
        del sys.modules['scenario']
        sys.path.remove(os.getcwd())
        os.remove('scenario.pyc')

        print test_name + ' pass'
    except TestFailure as tf:
        tf.say(test_name)
    except ImportError as ie:
        print test_name + ' fail:\n' + \
              '\tunable to import ' + test_name + '/scenario.py' 
    finally:
        os.chdir(topdir)

