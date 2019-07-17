## com.ibm.streamsx.monitoring.metrics.sample.MetricsSource

This sample SPL application demonstrates the use of MetricsSource operator.

### Use

#### Launch with python script

````
cd ..
python3 launch_metrics_monitor_sample.py
````

#### Launch with Streams Console

Build **Monitor** and **SampleJob** applications:

`make`

###### Run Monitor application

Launch the **Monitor** application first. 

Update `user` and `password` submission parameters for your Streams environment.

Alternative you can specify the `instanceId`, `connectionURL`, `user`, `password`, and `filterDocument` properties in an [application configuration](https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.0/com.ibm.streams.admin.doc/doc/creating-secure-app-configs.html).
Advantage of the use of an application configuration is, that you can update the `filterDocument` at runtime.

##### Run sample application to be monitored

Afterwards launch the **SampleJob** application to be monitored in distributed mode.


### Result

Verify the metrics notification tuples in the console output.

*In the Streams Console, go to the Log Viewer and Click on the PE's Console Log to view output.*

The MetricsSource operator emits a metric tuple for each metric, for which the operator identifies a changed value.
After each scan cycle, the operator emits a WindowMarker to this port.

    MetricNotification: {instanceId="StreamsInstance",jobId=6,jobName="com.ibm.streamsx.monitoring.metrics.sample.MetricsSource::SampleJob_6",resource="streamshost.ibm.com",peId=10,origin=OperatorOutputPort,operatorName="Numbers",channel=-1,portIndex=0,connectionId="",metricType="system",metricKind="counter",metricName="nTuplesSubmitted",metricValue=3,lastTimeRetrieved=1505995655000}
    ...
    MetricNotifications: WindowMarker


### Clean:

`make clean`

