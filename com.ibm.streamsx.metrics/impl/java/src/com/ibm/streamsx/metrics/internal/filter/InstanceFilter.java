package com.ibm.streamsx.metrics.internal.filter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

final class InstanceFilter extends PatternMatcher implements Filter {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(InstanceFilter.class.getName());

	/**
	 * An instance has many jobs.
	 */
	protected Map<String /* regular expression */, JobFilter> _jobFilters = new HashMap<>();

	public InstanceFilter(String regularExpression, Set<Filter> filters) throws PatternSyntaxException {
		super(regularExpression);
		for(Filter filter : filters) {
			JobFilter jobFilter = (JobFilter)filter;
			_jobFilters.put(jobFilter.getRegularExpression(), jobFilter);
		}
	}

	public boolean matchesInstanceId(String instanceId) {
		boolean matches = matches(instanceId);
		return matches;
	}

	public boolean matchesJobName(String instanceId, String jobName) {
		boolean matches = matchesInstanceId(instanceId) && (_jobFilters.size() > 0);
		if (matches) {
			for(JobFilter filter : _jobFilters.values()) {
				matches = filter.matchesJobName(jobName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesOperatorName(String instanceId, String jobName, String operatorName) {
		boolean matches = matchesInstanceId(instanceId) && (_jobFilters.size() > 0);
		if (matches) {
			for(JobFilter filter : _jobFilters.values()) {
				matches = filter.matchesOperatorName(jobName, operatorName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesOperatorMetricName(String instanceId, String jobName, String operatorName, String metricName) {
		boolean matches = matchesInstanceId(instanceId) && (_jobFilters.size() > 0);
		if (matches) {
			for(JobFilter filter : _jobFilters.values()) {
				matches = filter.matchesOperatorMetricName(jobName, operatorName, metricName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesOperatorInputPortIndex(String instanceId, String jobName, String operatorName, Integer portIndex) {
		boolean matches = matchesInstanceId(instanceId) && (_jobFilters.size() > 0);
		if (matches) {
			for(JobFilter filter : _jobFilters.values()) {
				matches = filter.matchesOperatorInputPortIndex(jobName, operatorName, portIndex);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesOperatorOutputPortIndex(String instanceId, String jobName, String operatorName, Integer portIndex) {
		boolean matches = matchesInstanceId(instanceId) && (_jobFilters.size() > 0);
		if (matches) {
			for(JobFilter filter : _jobFilters.values()) {
				matches = filter.matchesOperatorOutputPortIndex(jobName, operatorName, portIndex);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeMetricName(String instanceId, String jobName, String metricName) {
		boolean matches = matchesInstanceId(instanceId) && (_jobFilters.size() > 0);
		if (matches) {
			for(JobFilter filter : _jobFilters.values()) {
				matches = filter.matchesPeMetricName(jobName, metricName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeInputPortIndex(String instanceId, String jobName, Integer portIndex) {
		boolean matches = matchesInstanceId(instanceId) && (_jobFilters.size() > 0);
		if (matches) {
			for(JobFilter filter : _jobFilters.values()) {
				matches = filter.matchesPeInputPortIndex(jobName, portIndex);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeOutputPortIndex(String instanceId, String jobName, Integer portIndex) {
		boolean matches = matchesInstanceId(instanceId) && (_jobFilters.size() > 0);
		if (matches) {
			for(JobFilter filter : _jobFilters.values()) {
				matches = filter.matchesPeOutputPortIndex(jobName, portIndex);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

}
