package com.ibm.streamsx.metrics.internal.filter;

import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

final class DomainFilter extends Filter {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(DomainFilter.class.getName());

	public DomainFilter(String regularExpression) throws PatternSyntaxException {
		super(regularExpression);
	}

	public void add(String instanceNameRegEx, String jobNameRegEx, String operatorNameRegEx, String metricNameRegEx) {
		if (instanceNameRegEx != null) {
			if (!_filters.containsKey(instanceNameRegEx)) {
				addFilter(new InstanceFilter(instanceNameRegEx));
			}
			((InstanceFilter)_filters.get(instanceNameRegEx)).add(jobNameRegEx, operatorNameRegEx, metricNameRegEx);
		}
	}

	public boolean matches(String domainName, String instanceName, String jobName, String operatorName, String metricName, int stopLevel) {
		final boolean isInfoEnabled = _trace.isInfoEnabled();
		if(isInfoEnabled) {
			_trace.info(String.format("matches(domainName=%s, instanceName=%s, jobName=%s, operatorName=%s, metricName=%s, stopLevel=%d): %s\n", domainName, instanceName, jobName, operatorName, metricName, stopLevel, _regularExpression));
		}
		boolean matches = (domainName != null) && _matcher.reset(domainName).matches();
		if (matches) {
			--stopLevel;
			if (stopLevel > 0) {
				for(Filter filter : _filters.values()) {
					matches = ((InstanceFilter)filter).matches(instanceName, jobName, operatorName, metricName, stopLevel);
					if (matches) {
						break;
					}
				}
			}
		}
		if(isInfoEnabled) {
			_trace.info(String.format("matches(domainName=%s, instanceName=%s, jobName=%s, operatorName=%s, metricName=%s, stopLevel=%d): %s -> %s\n", domainName, instanceName, jobName, operatorName, metricName, stopLevel, _regularExpression, Boolean.toString(matches)));
		}
		return matches;
	}

}
