# streamsx.monitoring

The **com.ibm.streamsx.monitoring** provides capabilities to create applications that monitor IBM Streams and its applications. 

The toolkit contains operators that uses the **JMX API** to monitor applications:
* **com.ibm.streamsx.monitoring.metrics::MetricsSource** retrieves metrics from one or more jobs and provides them as tuple stream.
* **com.ibm.streamsx.monitoring.jobs::JobStatusMonitor** receives notifications of PE status changes from one or more jobs and provides them as tuple stream.
* **com.ibm.streamsx.monitoring.system::LogSource** receives notifications of application error and warning logs and provides them as tuple stream.

## Documentation

Find the full documentation [here](https://ibmstreams.github.io/streamsx.monitoring/).

## IBM Streams 5.x - IBM Cloud Pak for Data 

This toolkit is compatible with the IBM Streams version 5.x running in IBM Cloud Pak for Data.
* Monitoring toolkit release for Streams 5.x: [latest](https://github.com/IBMStreams/streamsx.monitoring/releases/latest)
* [SPLDoc](https://ibmstreams.github.io/streamsx.monitoring/doc/spldoc/html/)

## IBM Streams 4.3.x - Streaming Analytics service on IBM Cloud

For IBM Streams version 4.3.x and Streaming Analytics service on IBM Cloud you need to use version **2** of the `com.ibm.streamsx.monitoring` toolkit.
* Monitoring toolkit release for Streams 4.3: [v2.0.1](https://github.com/IBMStreams/streamsx.monitoring/releases/tag/v2.0.1)
* [SPLDoc](https://ibmstreams.github.io/streamsx.monitoring/doc/spldoc2/html/)
