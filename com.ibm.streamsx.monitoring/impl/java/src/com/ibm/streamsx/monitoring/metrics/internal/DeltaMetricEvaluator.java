//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.metrics.internal;

import com.ibm.streams.management.Metric;

/**
 * This metric evaluator marks a metric as submittable if the delta between
 * the last and the current value equals or exceeds the specified allowed
 * delta.
 */
public class DeltaMetricEvaluator implements IMetricEvaluator {

	private long _delta = 1;
	
	private Long _lastValue = null;
	
	public DeltaMetricEvaluator() {
	}

	public DeltaMetricEvaluator(long delta) {
		if (delta <= 0) {
			throw new IllegalArgumentException("DeltaMetricEvaluator(delta=" + delta + ")");
		}
		_delta = delta;
	}
	
	@Override
	public boolean isSubmittable(Metric metric) {
		return (_lastValue == null) || (Math.abs(_lastValue - metric.getValue()) >= _delta);
	}

	@Override
	public void updateStatus(Metric metric) {
		_lastValue = metric.getValue();
	}

}
