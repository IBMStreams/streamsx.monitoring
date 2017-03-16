# streamsx.metrics

The **com.ibm.streamsx.metrics** toolkit supports monitoring Streams applications
by providing the **MetricsSource** operator that can produce a stream of metrics.

If you want to monitor Streams or Streams applications with non-Streams
applications, you can use the following API:

* [JMX](http://www.ibm.com/support/knowledgecenter/SSCRJU_4.2.0/com.ibm.streams.ref.doc/doc/jmxapi.html)
* [REST](http://www.ibm.com/support/knowledgecenter/SSCRJU_4.2.0/com.ibm.streams.restapi.doc/doc/restapis.html)

The com.ibm.streamsx.metrics toolkit completes this list of APIs with the
**com.ibm.streamsx.metrics::MetricsSource** operator that uses the **JMX API**
to retrieve metrics from one or more jobs, and provides them as tuple stream.

## Documentation

Find the full documentation [here](https://ibmstreams.github.io/streamsx.metrics/).

## Streaming Analytics service on IBM Bluemix

This toolkit is compatible with the Streaming Analytics service on Bluemix.

It is recommended to create an Application Configuration before launching a sample application using the MetricsSource operator.

1. Build and launch the sample application **com.ibm.streamsx.metrics.sample.SetupApplicationConfiguration**
2. Build and launch the sample application **com.ibm.streamsx.metrics.sample.MetricsSource.ApplicationConfiguration**


