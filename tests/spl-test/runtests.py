#!/usr/bin/env python

# Copyright (C) 2015 International Business Machines Corporation. 
# All Rights Reserved.

import sys
import testharness as th
from optparse import OptionParser

def read_tests(name):
    file = open(name, 'r')
    tests = []
    for line in file:
        tests.append(line.rstrip())
    return tests 

def main():
    parser = OptionParser()
    parser.add_option('-t', '--test', dest='test',
                      help='test to run', metavar='INST')
    parser.add_option('-f', '--file', dest='file',
                      help='file of test names', metavar='INST')
    (options, args) = parser.parse_args()

    if not options.test and not options.file:
        print 'Error: Must specify either a test or a file of test names.'
        parser.print_help()
        sys.exit(1)

    test_list = []
    if options.test:
        test_list.append(options.test)
    if options.file:
        test_list += read_tests(options.file)

    for t in test_list:
        th.testharness(t)

if __name__ == '__main__':
    main()
