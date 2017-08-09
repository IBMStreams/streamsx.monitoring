//
// ****************************************************************************
// * Copyright (C) 2016, 2017, International Business Machines Corporation    *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.jmx.internal.filters;

import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

final class MetricFilter extends PatternMatcher {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(MetricFilter.class.getName());

	public MetricFilter(String regularExpression) throws PatternSyntaxException {
		super(regularExpression);
	}

	public boolean matchesMetricName(String metricName) {
		boolean matches = matches(metricName);
		return matches;
	}
	
}
