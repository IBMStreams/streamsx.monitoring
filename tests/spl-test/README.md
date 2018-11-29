# Setup for tests

The test scripts require **python 3.x** for python unittest. Ensure that `python3` command is in your `PATH`.

You need to set the following environment variables:

* `STREAMS_USERNAME` - user name for JMX connection
* `STREAMS_PASSWORD` - password for JMX connection

## Optional 

The test suites `test_jmx_reconnect` require the following environment variables:

* `TEST_DOMAIN` - domain ID for a second *Streams Domain*. It is required that the property `jmx.port` is **not** set to value `0`.
* `TEST_INSTANCE` - instance ID of a *Streams Instance* in the `TEST_DOMAIN`

If not set, then the test suites are skipped.

## Before launching the test

**The test scripts does not create, modify or delete a Streams Domain or Instance.**

* It is required that the domain (`STREAMS_DOMAIN_ID`) and instance (`STREAMS_INSTANCE_ID`) are started.
* *If present, the domain (`TEST_DOMAIN`) and instance (`TEST_INSTANCE`) should be stopped.*


# Run the tests
```
ant test
```

To run a single test, select one of the test suites, for example:
```
python3 -m unittest JobStatusMonitor/test_jmx_reconnect/test_jmx_reconnect.py -v
```

# Clean-up

Delete generated files of test suites.
```
ant clean
```
