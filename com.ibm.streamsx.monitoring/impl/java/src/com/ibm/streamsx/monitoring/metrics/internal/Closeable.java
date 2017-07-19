package com.ibm.streamsx.monitoring.metrics.internal;

public interface Closeable {

	default void close() throws Exception {
	}
}
