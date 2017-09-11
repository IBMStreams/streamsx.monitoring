---
title: "Build and launch sample application"
permalink: /docs/user/sample/
excerpt: "How to build and run a sample."
last_modified_at: 2017-09-11T12:37:48-04:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "userdocs"
---
{% include toc %}
{%include editme %}

# Precondition

* pre-built toolkit
* Streams domain and instance is running

# Build the sample

Change to the sample directory, e.g. `
samples/com.ibm.streamsx.monitoring.sample.MetricsSource.MemoryMetrics`

This sample application monitors the metrics *nMemoryConsumption* and *nResidentMemoryConsumption* of all jobs and PEs in the specified domain.

Build the sample application with the following command:

    make

# Run the sample

The sample application requires the following submission time parameters:

* user - Specifies the user that is required for the JMX connection.
* password - Specifies the password that is required for the JMX connection.

Launch the sample application, e.g.

    streamtool submitjob output/com.ibm.streamsx.monitoring.sample.MetricsSource.MemoryMetrics.Main.sab -P user=streamsuser -P password=streamsuser

The application traces the received metric value change notifications.
You could display them with the following command, e.g. if your job has PE Id 0:

    streamtool viewlog --pe 0


