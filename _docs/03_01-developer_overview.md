---
title: "Getting Started"
permalink: /docs/developer/overview/
excerpt: "Contributing to this toolkits development."
last_modified_at: 2017-09-11T12:37:48-04:00
redirect_from:
   - /theme-setup/
sidebar:
   nav: "developerdocs"
---
{% include toc %}
{% include editme %}

# Prerequisite

This toolkit uses Apache Ant 1.8 (or later) to build.

This toolkit requires

* IBM Streams 4.1 or later
* Java 1.8

It is recommended to use the Java version that is part of the Streams installation. For example, add the following commands to the .bashrc file:

    source PATH_TO_STREAMS_INSTALLATION/bin/streamsprofile.sh
    export JAVA_HOME=${STREAMS_INSTALL}/java
    export PATH=${JAVA_HOME}/bin:$PATH

# Build the toolkit

Run the following command in the `streamsx.monitoring` directory:

    ant all

# Build the samples

Run the following command in the `streamsx.monitoring` directory to build all projects in the `samples` directory:

    ant build-all-samples
