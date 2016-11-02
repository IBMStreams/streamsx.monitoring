package com.ibm.streamsx.metrics.internal.filter;

import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

final class JobFilter extends Filter {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(DomainFilter.class.getName());

	public JobFilter(String regularExpression) throws PatternSyntaxException {
		super(regularExpression);
	}

	public void add(String operatorNameRegEx, String metricNameRegEx) {
		if (operatorNameRegEx != null) {
			if (!_filters.containsKey(operatorNameRegEx)) {
				addFilter(new OperatorFilter(operatorNameRegEx));
			}
			((OperatorFilter)_filters.get(operatorNameRegEx)).add(metricNameRegEx);
		}
	}

	public boolean matches(String jobName, String operatorName, String metricName, int stopLevel) {
		final boolean isInfoEnabled = _trace.isInfoEnabled();
		if(isInfoEnabled) {
			_trace.info(String.format("matches(jobName=%s, operatorName=%s, metricName=%s, stopLevel=%d): %s\n", jobName, operatorName, metricName, stopLevel, _regularExpression));
		}
		boolean matches = (jobName != null) && _matcher.reset(jobName).matches();
		if (matches) {
			--stopLevel;
			if (stopLevel > 0) {
				for(Filter filter : _filters.values()) {
					matches = ((OperatorFilter)filter).matches(operatorName, metricName, stopLevel);
					if (matches) {
						break;
					}
				}
			}
		}
		if(isInfoEnabled) {
			_trace.info(String.format("matches(jobName=%s, operatorName=%s, metricName=%s, stopLevel=%d): %s -> %s\n", jobName, operatorName, metricName, stopLevel, _regularExpression, Boolean.toString(matches)));
		}
		return matches;
	}

}
