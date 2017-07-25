# streamsx.monitoring

The **com.ibm.streamsx.monitoring** toolkit supports monitoring Streams applications
by providing the **MetricsSource** operator that can produce a stream of metrics.

If you want to monitor Streams or Streams applications with non-Streams
applications, you can use the following API:

* [JMX](http://www.ibm.com/support/knowledgecenter/SSCRJU_4.2.0/com.ibm.streams.ref.doc/doc/jmxapi.html)
* [REST](http://www.ibm.com/support/knowledgecenter/SSCRJU_4.2.0/com.ibm.streams.restapi.doc/doc/restapis.html)

The com.ibm.streamsx.monitoring toolkit completes this list of APIs with the
**com.ibm.streamsx.monitoring.metrics::MetricsSource** operator that uses the **JMX API**
to retrieve metrics from one or more jobs, and provides them as tuple stream.

## Getting Started

https://github.com/IBMStreams/streamsx.monitoring/wiki/Getting-Started

## Documentation

Find the full documentation [here](https://ibmstreams.github.io/streamsx.monitoring/).

## Streaming Analytics service on IBM Bluemix

This toolkit is compatible with the Streaming Analytics service on Bluemix.


