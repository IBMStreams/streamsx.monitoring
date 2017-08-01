package com.ibm.streamsx.monitoring.jobs.internal;

public interface Closeable {

	default void close() throws Exception {
	}
}
