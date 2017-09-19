## com.ibm.streamsx.monitoring.sample.JobStatusMonitor

This sample SPL application demonstrates the use of JobStatusMonitor operator to get the notification of PE status changes and added/removed jobs.

### Use

Build Monitor and SampleJob applications:

`make`

### Run Monitor application

Launch the Monitor application in distributed mode first.

For example with Update `user` and `password` submission parameters:

`streamtool submitjob output/monitor/com.ibm.streamsx.monitoring.jobs.sample.Monitor.sab -P user=streamsadmin -P password=password`

Update `user` and `password` submission parameters for your Streams environment.

Alternative you can specify the `domainId`, `connectionURL`, `user`, `password`, and `filterDocument` properties in an application configuration.
Advantage of the use of an application configuration is, that you can update the `filterDocument` at runtime.

`streamtool submitjob output/monitor/com.ibm.streamsx.monitoring.jobs.sample.Monitor.sab -P applicationConfigurationName=com.ibm.streamsx.monitoring.JobStatusSource.ApplicationConfiguration`

### Run sample application to be monitored

Afterwards launch the Sample application to be monitored.
`streamtool submitjob ./output/sample/com.ibm.streamsx.monitoring.jobs.sample.SampleJob.sab`

### Clean:

`make clean`

