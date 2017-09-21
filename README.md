# streamsx.monitoring

The **com.ibm.streamsx.monitoring** provides capabilities to create applications that monitor IBM Streams and its applications. 

The toolkit contains operators that uses the **JMX API** to monitor applications:
* **com.ibm.streamsx.monitoring.metrics::MetricsSource** retrieves metrics from one or more jobs and provides them as tuple stream.
* **com.ibm.streamsx.monitoring.jobs::JobStatusMonitor** receives notifications of PE status changes from one or more jobs and provides them as tuple stream.
* **com.ibm.streamsx.monitoring.system::LogSource** receives notifications of application error and warning logs and provides them as tuple stream.

## Documentation

Find the full documentation [here](https://ibmstreams.github.io/streamsx.monitoring/).

## Streaming Analytics service on IBM Bluemix

This toolkit is compatible with the Streaming Analytics service on Bluemix.


