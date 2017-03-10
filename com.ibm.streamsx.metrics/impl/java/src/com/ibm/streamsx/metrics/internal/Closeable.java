package com.ibm.streamsx.metrics.internal;

public interface Closeable {

	default void close() throws Exception {
	}
}
