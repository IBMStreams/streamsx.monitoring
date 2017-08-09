//
// ****************************************************************************
// * Copyright (C) 2016, 2017, International Business Machines Corporation    *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.jmx.internal.filters;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

final class InstanceFilter extends PatternMatcher {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(InstanceFilter.class.getName());

	/**
	 * An instance has many jobs.
	 */
	protected Map<String /* regular expression */, JobFilter> _jobFilters = new HashMap<>();

	public InstanceFilter(String regularExpression, Set<JobFilter> filters) throws PatternSyntaxException {
		super(regularExpression);
		for(JobFilter jobFilter : filters) {
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

	public boolean matchesOperatorInputPortMetricName(String instanceId, String jobName, String operatorName, Integer portIndex, String metricName) {
		boolean matches = matchesInstanceId(instanceId) && (_jobFilters.size() > 0);
		if (matches) {
			for(JobFilter filter : _jobFilters.values()) {
				matches = filter.matchesOperatorInputPortMetricName(jobName, operatorName, portIndex, metricName);
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

	public boolean matchesOperatorOutputPortMetricName(String instanceId, String jobName, String operatorName, Integer portIndex, String metricName) {
		boolean matches = matchesInstanceId(instanceId) && (_jobFilters.size() > 0);
		if (matches) {
			for(JobFilter filter : _jobFilters.values()) {
				matches = filter.matchesOperatorOutputPortMetricName(jobName, operatorName, portIndex, metricName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeId(String instanceId, String jobName, BigInteger peId) {
		boolean matches = matchesInstanceId(instanceId) && (_jobFilters.size() > 0);
		if (matches) {
			for(JobFilter filter : _jobFilters.values()) {
				matches = filter.matchesPeId(jobName, peId);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeMetricName(String instanceId, String jobName, BigInteger peId, String metricName) {
		boolean matches = matchesInstanceId(instanceId) && (_jobFilters.size() > 0);
		if (matches) {
			for(JobFilter filter : _jobFilters.values()) {
				matches = filter.matchesPeMetricName(jobName, peId, metricName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeInputPortIndex(String instanceId, String jobName, BigInteger peId, Integer portIndex) {
		boolean matches = matchesInstanceId(instanceId) && (_jobFilters.size() > 0);
		if (matches) {
			for(JobFilter filter : _jobFilters.values()) {
				matches = filter.matchesPeInputPortIndex(jobName, peId, portIndex);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeInputPortMetricName(String instanceId, String jobName, BigInteger peId, Integer portIndex, String metricName) {
		boolean matches = matchesInstanceId(instanceId) && (_jobFilters.size() > 0);
		if (matches) {
			for(JobFilter filter : _jobFilters.values()) {
				matches = filter.matchesPeInputPortMetricName(jobName, peId, portIndex, metricName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeOutputPortIndex(String instanceId, String jobName, BigInteger peId, Integer portIndex) {
		boolean matches = matchesInstanceId(instanceId) && (_jobFilters.size() > 0);
		if (matches) {
			for(JobFilter filter : _jobFilters.values()) {
				matches = filter.matchesPeOutputPortIndex(jobName, peId, portIndex);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeOutputPortMetricName(String instanceId, String jobName, BigInteger peId, Integer portIndex, String metricName) {
		boolean matches = matchesInstanceId(instanceId) && (_jobFilters.size() > 0);
		if (matches) {
			for(JobFilter filter : _jobFilters.values()) {
				matches = filter.matchesPeOutputPortMetricName(jobName, peId, portIndex, metricName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}
	
	public boolean matchesPeConnectionId(String instanceId, String jobName, BigInteger peId, String connectionId) {
		boolean matches = matchesInstanceId(instanceId) && (_jobFilters.size() > 0);
		if (matches) {
			for(JobFilter filter : _jobFilters.values()) {
				matches = filter.matchesPeConnectionId(jobName, peId, connectionId);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}
	
	public boolean matchesPeConnectionMetricName(String instanceId, String jobName, BigInteger peId, String peConnection, String metricName) {
		boolean matches = matchesInstanceId(instanceId) && (_jobFilters.size() > 0);
		if (matches) {
			for(JobFilter filter : _jobFilters.values()) {
				matches = filter.matchesPeConnectionMetricName(jobName, peId, peConnection, metricName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

}
