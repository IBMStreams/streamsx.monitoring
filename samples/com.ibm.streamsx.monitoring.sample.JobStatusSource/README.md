## com.ibm.streamsx.monitoring.sample.JobStatusSource

This sample SPL application demonstrates the use of JobStatusSource operator to get the notification of PE status changes and added/removed jobs.

Use

Build Monitor and SampleJob applications:

`make`

Run:

Launch the Monitor application in distributed mode first.
`./output/monitor/com.ibm.streamsx.monitoring.jobs.sample.Monitor.sab`

Afterwards launch the Sample application to be monitored.
`./output/sample/com.ibm.streamsx.monitoring.jobs.sample.SampleJob.sab`


Clean:

`make clean`

