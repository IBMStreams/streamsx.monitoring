## com.ibm.streamsx.monitoring.sample.LogSource

This sample SPL application demonstrates the use of LogSource operator to get the notification of log messages.

Use

Build Monitor and SampleJob applications:

`make`

Run:

Launch the Monitor application in distributed mode first and provide the `user` and `password` submission parameters.
`./output/monitor/com.ibm.streamsx.monitoring.sample.logs.Monitor.sab`

Afterwards launch the Sample application to be monitored.
`./output/sample/com.ibm.streamsx.monitoring.sample.logs.SampleJob.sab`


Clean:

`make clean`

