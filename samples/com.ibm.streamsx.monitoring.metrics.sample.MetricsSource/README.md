## com.ibm.streamsx.monitoring.metrics.sample.MetricsSource

This sample SPL application demonstrates the use of MetricsSource operator.

### Use

Build **Monitor** and **SampleJob** applications:

`make`

### Run Monitor application

Launch the **Monitor** application first. 

#### Standalone mode

The monitor application in standalone mode can not determine the `domainId`. Therefore you need to specify the `domainId` parameter.

Update `domainId`, `user` and `password` parameters for your Streams environment.

`./output/monitor/bin/standalone domainId=StreamsDomain user=streamsadmin password=password`

#### Distributed mode

For example with `user` and `password` submission parameters:

`streamtool submitjob output/monitor/com.ibm.streamsx.monitoring.metrics.sample.MetricsSource.Monitor.sab -P user=streamsadmin -P password=password`

Update `user` and `password` submission parameters for your Streams environment.

Alternative you can specify the `domainId`, `connectionURL`, `user`, `password`, and `filterDocument` properties in an [application configuration](https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.0/com.ibm.streams.admin.doc/doc/creating-secure-app-configs.html).
Advantage of the use of an application configuration is, that you can update the `filterDocument` at runtime.

***Application configuration is supported in distributed mode only.***

`streamtool submitjob output/monitor/com.ibm.streamsx.monitoring.metrics.sample.MetricsSource.Monitor.sab -P applicationConfigurationName=com.ibm.streamsx.monitoring.metrics.MetricsSource.ApplicationConfiguration`

### Run sample application to be monitored

Afterwards launch the **SampleJob** application to be monitored in distributed mode.

`streamtool submitjob output/sample/com.ibm.streamsx.monitoring.metrics.sample.MetricsSource.SampleJob.sab`

### Result

Verify the metrics notification tuples in the console output.

*In the Streaming Analytics service, go to the Log Viewer and Click on the PE's Console Log to view output.*

The MetricsSource operator emits a metric tuple for each metric, for which the operator identifies a changed value.
After each scan cycle, the operator emits a WindowMarker to this port.

    MetricNotification: {domainId="StreamsDomain",instanceId="StreamsInstance",jobId=6,jobName="com.ibm.streamsx.monitoring.metrics.sample.MetricsSource::SampleJob_6",resource="streamshost.ibm.com",peId=10,origin=OperatorOutputPort,operatorName="Numbers",channel=-1,portIndex=0,connectionId="",metricType="system",metricKind="counter",metricName="nTuplesSubmitted",metricValue=3,lastTimeRetrieved=1505995655000}
    ...
    MetricNotifications: WindowMarker


### Clean:

`make clean`

