## com.ibm.streamsx.monitoring.jobs.sample.JobStatusMonitor

This sample SPL application demonstrates the use of JobStatusMonitor operator to get the notification of PE status changes and added/removed jobs.

### Use

#### Launch with python script

````
cd ..
python3 launch_job_status_monitor_sample.py
````

#### Launch with Streams Console

Build **Monitor** and **SampleJob** applications:

`make`

##### Run Monitor application

Launch the **Monitor** application first. 

Update `user` and `password` submission parameters for your Streams environment.

Alternative you can specify the `instanceId`, `connectionURL`, `user`, `password`, and `filterDocument` properties in an [application configuration](https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.0/com.ibm.streams.admin.doc/doc/creating-secure-app-configs.html).
Advantage of the use of an application configuration is, that you can update the `filterDocument` at runtime.

##### Run sample application to be monitored

Afterwards launch the **SampleJob** application to be monitored in distributed mode.

### Result

Verify the notification events in the console output.

*In the Streams Console, go to the Log Viewer and Click on the PE's Console Log to view output.*

One PE of the sample application has been forced to restart and the output contains "com.ibm.streams.management.pe.changed" notifications:

    ...
    {notifyType="com.ibm.streams.management.pe.changed",instanceId="StreamsInstance",jobId=0,jobName="com.ibm.streamsx.monitoring.jobs.sample::SampleJob_0",resource="streamshost.ibm.com",peId=2,peHealth="partiallyUnhealthy",peStatus="restarting",eventTimestamp=(1505980604,904000000,0)}
    ...
    {notifyType="com.ibm.streams.management.pe.changed",instanceId="StreamsInstance",jobId=0,jobName="com.ibm.streamsx.monitoring.jobs.sample::SampleJob_0",resource="streamshost.ibm.com",peId=2,peHealth="healthy",peStatus="running",eventTimestamp=(1505980606,474000000,0)}


### Clean:

`make clean`

