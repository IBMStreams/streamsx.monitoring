package com.ibm.streamsx.monitoring.jmx.internal;

public interface Closeable {

	default void close() throws Exception {
	}
}
