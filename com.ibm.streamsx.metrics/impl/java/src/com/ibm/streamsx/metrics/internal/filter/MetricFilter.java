package com.ibm.streamsx.metrics.internal.filter;

import java.util.regex.PatternSyntaxException;

final class MetricFilter extends Filter {

	public MetricFilter(String regularExpression) throws PatternSyntaxException {
		super(regularExpression);
	}

	@Override
	public void addFilter(Filter filter) { }

	public boolean matches(String metricName, int stopLevel) {
		boolean matches = (stopLevel == 0) || ((metricName != null) && _matcher.reset(metricName).matches());
		return matches;
	}
	
}
