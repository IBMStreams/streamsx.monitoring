package com.ibm.streamsx.metrics.internal.filter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

final class DomainFilter extends PatternMatcher implements Filter {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(DomainFilter.class.getName());

	/**
	 * A domain has many instances.
	 */
	protected Map<String /* regular expression */, InstanceFilter> _instanceFilters = new HashMap<>();

	public DomainFilter(String regularExpression, Set<Filter> filters) throws PatternSyntaxException {
		super(regularExpression);
		for(Filter filter : filters) {
			InstanceFilter instanceFilter = (InstanceFilter)filter;
			_instanceFilters.put(instanceFilter.getRegularExpression(), instanceFilter);
		}
	}

	public boolean matchesDomainId(String domainId) {
		boolean matches = matches(domainId);
		return matches;
	}

	public boolean matchesInstanceId(String domainId, String instanceId) {
		boolean matches = matchesDomainId(domainId);
		if (matches) {
			for(InstanceFilter filter : _instanceFilters.values()) {
				matches = filter.matchesInstanceId(instanceId);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesJobName(String domainId, String instanceId, String jobName) {
		boolean matches = matchesDomainId(domainId);
		if (matches) {
			for(InstanceFilter filter : _instanceFilters.values()) {
				matches = filter.matchesJobName(instanceId, jobName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesOperatorName(String domainId, String instanceId, String jobName, String operatorName) {
		boolean matches = matchesDomainId(domainId);
		if (matches) {
			for(InstanceFilter filter : _instanceFilters.values()) {
				matches = filter.matchesOperatorName(instanceId, jobName, operatorName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesOperatorMetricName(String domainId, String instanceId, String jobName, String operatorName, String metricName) {
		boolean matches = matchesDomainId(domainId);
		if (matches) {
			for(InstanceFilter filter : _instanceFilters.values()) {
				matches = filter.matchesOperatorMetricName(instanceId, jobName, operatorName, metricName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesOperatorInputPortIndex(String domainId, String instanceId, String jobName, String operatorName, Integer portIndex) {
		boolean matches = matchesDomainId(domainId);
		if (matches) {
			for(InstanceFilter filter : _instanceFilters.values()) {
				matches = filter.matchesOperatorInputPortIndex(instanceId, jobName, operatorName, portIndex);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesOperatorOutputPortIndex(String domainId, String instanceId, String jobName, String operatorName, Integer portIndex) {
		boolean matches = matchesDomainId(domainId);
		if (matches) {
			for(InstanceFilter filter : _instanceFilters.values()) {
				matches = filter.matchesOperatorOutputPortIndex(instanceId, jobName, operatorName, portIndex);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeMetricName(String domainId, String instanceId, String jobName, String metricName) {
		boolean matches = matchesDomainId(domainId);
		if (matches) {
			for(InstanceFilter filter : _instanceFilters.values()) {
				matches = filter.matchesPeMetricName(instanceId, jobName, metricName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeInputPortIndex(String domainId, String instanceId, String jobName, Integer portIndex) {
		boolean matches = matchesDomainId(domainId);
		if (matches) {
			for(InstanceFilter filter : _instanceFilters.values()) {
				matches = filter.matchesPeInputPortIndex(instanceId, jobName, portIndex);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeOutputPortIndex(String domainId, String instanceId, String jobName, Integer portIndex) {
		boolean matches = matchesDomainId(domainId);
		if (matches) {
			for(InstanceFilter filter : _instanceFilters.values()) {
				matches = filter.matchesPeOutputPortIndex(instanceId, jobName, portIndex);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

}
