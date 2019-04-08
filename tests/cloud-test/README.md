# Setup for testing with ICP4D Streams instance

## Before launching the test


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
