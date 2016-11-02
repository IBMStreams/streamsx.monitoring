package com.ibm.streamsx.metrics.internal.filter;

import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

final class InstanceFilter extends Filter {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(InstanceFilter.class.getName());

	public InstanceFilter(String regularExpression) throws PatternSyntaxException {
		super(regularExpression);
	}

	public void add(String jobNameRegEx, String operatorNameRegEx, String metricNameRegEx) {
		if (jobNameRegEx != null) {
			if (!_filters.containsKey(jobNameRegEx)) {
				addFilter(new JobFilter(jobNameRegEx));
			}
			((JobFilter)_filters.get(jobNameRegEx)).add(operatorNameRegEx, metricNameRegEx);
		}
	}

	public boolean matches(String instanceName, String jobName, String operatorName, String metricName, int stopLevel) {
		final boolean isInfoEnabled = _trace.isInfoEnabled();
		if(isInfoEnabled) {
			_trace.info(String.format("matches(instanceName=%s, jobName=%s, operatorName=%s, metricName=%s, stopLevel=%d): %s\n", instanceName, jobName, operatorName, metricName, stopLevel, _regularExpression));
		}
		boolean matches = (instanceName != null) && _matcher.reset(instanceName).matches();
		if (matches) {
			--stopLevel;
			if (stopLevel > 0) {
			for(Filter filter : _filters.values()) {
					matches = ((JobFilter)filter).matches(jobName, operatorName, metricName, stopLevel);
					if (matches) {
						break;
					}
				}
			}
		}
		if(isInfoEnabled) {
			_trace.info(String.format("matches(instanceName=%s, jobName=%s, operatorName=%s, metricName=%s, stopLevel=%d): %s -> %s\n", instanceName, jobName, operatorName, metricName, stopLevel, _regularExpression, Boolean.toString(matches)));
		}
		return matches;
	}

}
