package com.ibm.streamsx.metrics.internal;

import com.ibm.streams.management.Metric;

/**
 * This metric evaluator always marks a metric as submittable. There is no
 * need to store any status data.
 */
public class PeriodicMetricEvaluator implements IMetricEvaluator {

	@Override
	public boolean isSubmittable(Metric metric) {
		return true;
	}

	@Override
	public void updateStatus(Metric metric) {
	}

}
