# Building this toolkit

## Prerequisite

This toolkit uses Apache Ant 1.8 (or later) to build.

This toolkit requires 

* IBM Streams 4.3 or later
* Java 1.8

It is recommended to use the Java version that is part of the Streams installation.
For example, add the following commands to the `.bashrc` file:

    source PATH_TO_STREAMS_INSTALLATION/bin/streamsprofile.sh
    export JAVA_HOME=${STREAMS_INSTALL}/java
    export PATH=${JAVA_HOME}/bin:$PATH

## Build

To build this toolkit run the `ant` command in this directory.

For developers of this toolkit:

The top-level build.xml contains two main targets:

* all - Builds and creates SPLDOC for the toolkit and samples. Developers should ensure this target is successful when creating a pull request.
* build-all-samples - Builds all samples. Developers should ensure this target is successful when creating a pull request.
