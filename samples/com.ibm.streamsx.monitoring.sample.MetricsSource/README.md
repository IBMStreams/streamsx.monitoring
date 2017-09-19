## com.ibm.streamsx.monitoring.sample.MetricsSource

This sample SPL application demonstrates the use of MetricsSource operator.

### Use

Build the sample applications:

`make`

### Run Distributed with user and password parameters

For example with Update `user` and `password` submission parameters:

`streamtool submitjob output/com.ibm.streamsx.monitoring.sample.MetricsSource.Main.sab -P user=streamsadmin -P password=password`

Update `user` and `password` submission parameters for your Streams environment.

Alternative you can specify the `domainId`, `connectionURL`, `user`, `password`, and `filterDocument` properties in an application configuration.
Advantage of the use of an application configuration is, that you can update the `filterDocument` at runtime.

`streamtool submitjob output/com.ibm.streamsx.monitoring.sample.MetricsSource.Main.sab -P applicationConfigurationName=com.ibm.streamsx.monitoring.MetricsSource.ApplicationConfiguration`

### Clean:

`make clean`

