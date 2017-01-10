//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.metrics.internal;

import com.ibm.streams.management.Metric;

/**
 *
 */
public interface IMetricEvaluator {

	/**
	 * Evaluate the metric to decide whether a tuple shall be emitted.
	 * 
	 * @param metric
	 * Specifies the current metric.
	 * 
	 * @return
	 * True if a tuple shall be emitted.
	 */
	public boolean isSubmittable(Metric metric);
	
	/**
	 * Save or update any status data that is needed to get a decision for
	 * the next metric, for example, store the current value as new last
	 * value for a delta metric evaluator.
	 * 
	 * @param metric
	 * Specifies the current metric.
	 */
	public void updateStatus(Metric metric);
	
}
