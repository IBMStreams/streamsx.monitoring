package com.ibm.streamsx.metrics.internal.filter;

import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

final class MetricFilter extends PatternMatcher implements Filter {

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
