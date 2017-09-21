## com.ibm.streamsx.monitoring.system.sample.SystemMonitorSource

This sample SPL application demonstrates the use of SystemMonitorSource operator to get the CPU and Memory usage from the current running system.

### Use

Build the **Monitor** application:

`make`

### Run Monitor application

#### Standalone mode

`./output/bin/standalone`

#### Distributed mode

`streamtool submitjob output/com.ibm.streamsx.monitoring.system.sample.SystemMonitorSource.Monitor.sab`

### Result

Verify the **SystemMonitorSource** tuples in the console output.

*In the Streaming Analytics service, go to the Log Viewer and Click on the PE's Console Log to view output.*

    Date and time , CPU usage % , Memory usage (%) , Memory total (KB) , Memory used (KB)
    ...
    2017.09.21 14:34:21 , 0.71 , 57.19 , 10773852 , 6161460


### Clean:

`make clean`

