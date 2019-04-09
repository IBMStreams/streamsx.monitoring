## com.ibm.streamsx.monitoring.system.sample.LogSource

This sample SPL application demonstrates the use of LogSource operator to get the notification of log messages.

### Use

#### Launch with python script

````
cd ..
python3 launch_logs_monitor_sample.py
````

#### Launch with Streams Console

Build **Monitor** and **SampleJob** applications:

`make`

###### Run Monitor application

Launch the **Monitor** application first. 

Update `user` and `password` submission parameters for your Streams environment.

Alternative you can specify the `instanceId`, `connectionURL`, `user` and `password` properties in an [application configuration](https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.0/com.ibm.streams.admin.doc/doc/creating-secure-app-configs.html).

###### Run sample application to be monitored

Afterwards launch the **SampleJob** application to be monitored in distributed mode.

### Result

Verify the application log notification tuples in the console output.

*In the Streams Console, go to the Log Viewer and Click on the PE's Console Log to view output.*

    ...
    {notifyType="com.ibm.streams.management.log.application.error",instanceId="StreamsInstance",jobId=0,resource="streamshost.ibm.com",peId=0,operatorName="Logger",sequence=109,eventTimestamp=(1506002769,408000000,0),message="This is error log #.43"}
    {notifyType="com.ibm.streams.management.log.application.warning",instanceId="StreamsInstance",jobId=0,resource="streamshost.ibm.com",peId=0,operatorName="Logger",sequence=110,eventTimestamp=(1506002770,471000000,0),message="This is warning log #.43"}
    ...


### Clean:

`make clean`

