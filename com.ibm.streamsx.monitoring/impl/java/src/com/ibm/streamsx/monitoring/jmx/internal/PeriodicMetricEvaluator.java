//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.jmx.internal;

import com.ibm.streams.management.Metric;

/**
 * This metric evaluator always marks a metric as submittable. There is no
 * need to store any status data and therefore, the class is implemented as
 * singleton.
 */
public class PeriodicMetricEvaluator implements IMetricEvaluator {

	/**
	 * The singleton object.
	 */
	private static final PeriodicMetricEvaluator _singleton = new PeriodicMetricEvaluator();
	
	/**
	 * @return
	 * The singleton object.
	 */
	public static final PeriodicMetricEvaluator getSingleton() {
		return _singleton;
	}
	
	/**
	 * The class is implemented as singleton. Therefore, the constructor is
	 * private to ensure that nobody else can create an object.
	 */
	private PeriodicMetricEvaluator() {
	}
	
	@Override
	public boolean isSubmittable(Metric metric) {
		return true;
	}

	@Override
	public void updateStatus(Metric metric) {
	}

}
