## com.ibm.streamsx.monitoring.system.sample.LogSource

This sample SPL application demonstrates the use of LogSource operator to get the notification of log messages.

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

`streamtool submitjob output/monitor/com.ibm.streamsx.monitoring.system.sample.LogSource.Monitor.sab -P user=streamsadmin -P password=password`

Update `user` and `password` submission parameters for your Streams environment.

Alternative you can specify the `domainId`, `connectionURL`, `user` and `password` properties in an [application configuration](https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.0/com.ibm.streams.admin.doc/doc/creating-secure-app-configs.html).

***Application configuration is supported in distributed mode only.***

`streamtool submitjob output/monitor/com.ibm.streamsx.monitoring.system.sample.LogSource.Monitor.sab -P applicationConfigurationName=com.ibm.streamsx.monitoring.system.LogSource.ApplicationConfiguration`

### Run sample application to be monitored

Afterwards launch the **SampleJob** application to be monitored in distributed mode.

`streamtool submitjob output/sample/com.ibm.streamsx.monitoring.system.sample.LogSource.SampleJob.sab`

### Result

Verify the application log notification tuples in the console output.

*In the Streaming Analytics service, go to the Log Viewer and Click on the PE's Console Log to view output.*

    ...
    {notifyType="com.ibm.streams.management.log.application.error",domainId="StreamsDomain",instanceId="StreamsInstance",jobId=0,resource="streamshost.ibm.com",peId=0,operatorName="Logger",sequence=109,eventTimestamp=(1506002769,408000000,0),message="This is error log #.43"}
    {notifyType="com.ibm.streams.management.log.application.warning",domainId="StreamsDomain",instanceId="StreamsInstance",jobId=0,resource="streamshost.ibm.com",peId=0,operatorName="Logger",sequence=110,eventTimestamp=(1506002770,471000000,0),message="This is warning log #.43"}
    ...


### Clean:

`make clean`

