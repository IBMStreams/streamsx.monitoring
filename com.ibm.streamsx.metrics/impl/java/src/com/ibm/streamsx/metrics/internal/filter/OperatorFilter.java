package com.ibm.streamsx.metrics.internal.filter;

import java.util.regex.PatternSyntaxException;

final class OperatorFilter extends Filter {

	public OperatorFilter(String regularExpression) throws PatternSyntaxException {
		super(regularExpression);
	}

	public void add(String metricNameRegEx) {
		if (metricNameRegEx != null) {
			if (!_filters.containsKey(metricNameRegEx)) {
				addFilter(new MetricFilter(metricNameRegEx));
			}
		}
	}

	public boolean matches(String operatorName, String metricName, int stopLevel) {
		boolean matches = (operatorName != null) && _matcher.reset(operatorName).matches();
		if (matches) {
			--stopLevel;
			if (stopLevel > 0) {
				for(Filter filter : _filters.values()) {
					matches = ((MetricFilter)filter).matches(metricName, stopLevel);
					if (matches) {
						break;
					}
				}
			}
		}
		return matches;
	}

}
