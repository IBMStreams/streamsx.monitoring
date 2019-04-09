# Setup for testing with ICP4D Streams instance

## Before launching the test

Setup your environment:

* Python 3.6 installed
* streamsx package 1.12.9 or later installed

Set `STREAMS_USERNAME`, `STREAMS_PASSWORD` and `STREAMS_REST_URL` environment variables.

### Create Application Configuration

Test applications require an application configuration with the name "monitoring".

# Run the tests
```
ant test
```

    python3 -u -m unittest test_monitoring_icp.TestDistributed.test_metrics_monitor

# Clean-up

Delete generated files of test suites.
```
ant clean
```
