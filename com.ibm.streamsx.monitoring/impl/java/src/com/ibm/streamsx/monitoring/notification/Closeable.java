package com.ibm.streamsx.monitoring.notification;

public interface Closeable {

	default void close() throws Exception {
	}
}
