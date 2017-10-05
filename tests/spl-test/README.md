# Setup for tests

You need to set the following environment variables:

* `JMX_USER` - user name for JMX connection
* `JMX_PASSWORD` - password for JMX connection

## Optional 

The test suites `test_jmx_reconnect` require the following environment variables:

* `TEST_DOMAIN` - domain ID for a second *Streams Domain*. It is required that the property `jmx.port` is not to value `0`.
* `TEST_INSTANCE` - instance ID of a *Streams Instance* in the `TEST_DOMAIN`

If not set, then the test suites are skipped.

The test scripts does not create, modify or delete Streams Instance or Domain.

Before launching the test:
* It is required that the domain (`STREAMS_DOMAIN_ID`) and instance (`STREAMS_INSTANCE_ID`) are started.
* **If present, the domain (`TEST_DOMAIN`) and instance (`TEST_INSTANCE`) should be stopped.**


# Run the tests
```
ant test
```


# Clean-up

Delete generated files of test suites.
```
ant clean
```
