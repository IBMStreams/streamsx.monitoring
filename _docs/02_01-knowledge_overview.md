---
title: "Toolkit technical background overview"
permalink: /docs/knowledge/overview/
excerpt: "Basic knowledge of the toolkits technical domain."
last_modified_at: 2017-09-11T12:37:48-04:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "knowledgedocs"
---
{% include toc %}
{% include editme %}

The com.ibm.streamsx.monitoring provides capabilities to create applications that monitor IBM Streams and its applications.

The toolkit contains operators that uses the JMX API to monitor applications:

* com.ibm.streamsx.monitoring.metrics::MetricsSource retrieves metrics from one or more jobs and provides them as tuple stream.
* com.ibm.streamsx.monitoring.jobs::JobStatusSource receives notifications of PE status changes from one or more jobs and provides them as tuple stream.
* com.ibm.streamsx.monitoring.system::LogSource receives notifications of application error and warning logs and provides them as tuple stream.
