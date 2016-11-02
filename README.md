# streamsx.metrics

The **com.ibm.streamsx.metrics** toolkit supports monitoring Streams applications
by providing the **MetricsSource** operator that can produce a stream of metrics.

IBM Streams supports standard and custom metrics that are assigned to operators
and operator ports in a Streams application, for example, the number of received
and sent tuples. These metrics can be monitored with the Streams console or
Streams Studio to identify whether a Streams application runs as expected or
whether it has some issues.

IBM Streams provides the
[spl.adapter::MetricsSink](http://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.0/com.ibm.streams.toolkits.doc/spldoc/dita/tk$spl/op$spl.adapter$MetricsSink.html)
operator to set metrics values from within a Streams applications, and SPL
functions in the [spl.utility](http://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.0/com.ibm.streams.toolkits.doc/spldoc/dita/tk$spl/ns$spl.utility.html)
namespace to create custom metrics, and to set and get metrics values:

* spl.utility::createCustomMetric
* spl.utility::getCustomMetricNames
* spl.utility::getCustomMetricValue
* spl.utility::getInputPortMetricValue
* spl.utility::getOutputPortMetricValue
* spl.utility::hasCustomMetric
* spl.utility::setCustomMetricValue

The functions are limited to the operator that owns a metric. An operator A
cannot access the metrics of an operator B.

If you want to monitor Streams or Streams applications with non-Streams
applications, you can use the following API:

* [JMX](http://www.ibm.com/support/knowledgecenter/SSCRJU_4.2.0/com.ibm.streams.ref.doc/doc/jmxapi.html)
* [REST](http://www.ibm.com/support/knowledgecenter/SSCRJU_4.2.0/com.ibm.streams.restapi.doc/doc/restapis.html)

The com.ibm.streamsx.metrics toolkit completes this list of APIs with the
**com.ibm.streamsx.metrics::MetricsSource** operator that uses the JMX API
to retrieve metrics from one or more jobs, and provides them as tuple stream.

This introduction and further details are described in the toolkit's info.xml
file, and the operator's implementation.

You can generate HTML from the SPLDOC with the following commands:

```
spl-make-toolkit -i com.ibm.streamsx.metrics
spl-make-doc -i com.ibm.streamsx.metrics
```

You can view the HTML pages with any browser, or, for example, the following
command:

```
firefox com.ibm.streamsx.metrics/doc/spldoc/html/index.html &
```
