//
// ****************************************************************************
// * Copyright (C) 2016, 2017, International Business Machines Corporation    *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.metrics.internal.filter;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

final class DomainFilter extends PatternMatcher {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(DomainFilter.class.getName());

	/**
	 * A domain has many instances.
	 */
	protected Map<String /* regular expression */, InstanceFilter> _instanceFilters = new HashMap<>();

	public DomainFilter(String regularExpression, Set<InstanceFilter> filters) throws PatternSyntaxException {
		super(regularExpression);
		for(InstanceFilter instanceFilter : filters) {
			_instanceFilters.put(instanceFilter.getRegularExpression(), instanceFilter);
		}
	}

	public boolean matchesDomainId(String domainId) {
		boolean matches = matches(domainId);
		return matches;
	}

	public boolean matchesInstanceId(String domainId, String instanceId) {
		boolean matches = matchesDomainId(domainId) && (_instanceFilters.size() > 0);
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
		boolean matches = matchesDomainId(domainId) && (_instanceFilters.size() > 0);
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
		boolean matches = matchesDomainId(domainId) && (_instanceFilters.size() > 0);
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
		boolean matches = matchesDomainId(domainId) && (_instanceFilters.size() > 0);
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
		boolean matches = matchesDomainId(domainId) && (_instanceFilters.size() > 0);
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

	public boolean matchesOperatorInputPortMetricName(String domainId, String instanceId, String jobName, String operatorName, Integer portIndex, String metricName) {
		boolean matches = matchesDomainId(domainId) && (_instanceFilters.size() > 0);
		if (matches) {
			for(InstanceFilter filter : _instanceFilters.values()) {
				matches = filter.matchesOperatorInputPortMetricName(instanceId, jobName, operatorName, portIndex, metricName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesOperatorOutputPortIndex(String domainId, String instanceId, String jobName, String operatorName, Integer portIndex) {
		boolean matches = matchesDomainId(domainId) && (_instanceFilters.size() > 0);
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

	public boolean matchesOperatorOutputPortMetricName(String domainId, String instanceId, String jobName, String operatorName, Integer portIndex, String metricName) {
		boolean matches = matchesDomainId(domainId) && (_instanceFilters.size() > 0);
		if (matches) {
			for(InstanceFilter filter : _instanceFilters.values()) {
				matches = filter.matchesOperatorOutputPortMetricName(instanceId, jobName, operatorName, portIndex, metricName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeId(String domainId, String instanceId, String jobName, BigInteger peId) {
		boolean matches = matchesDomainId(domainId) && (_instanceFilters.size() > 0);
		if (matches) {
			for(InstanceFilter filter : _instanceFilters.values()) {
				matches = filter.matchesPeId(instanceId, jobName, peId);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeMetricName(String domainId, String instanceId, String jobName, BigInteger peId, String metricName) {
		boolean matches = matchesDomainId(domainId) && (_instanceFilters.size() > 0);
		if (matches) {
			for(InstanceFilter filter : _instanceFilters.values()) {
				matches = filter.matchesPeMetricName(instanceId, jobName, peId, metricName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeInputPortIndex(String domainId, String instanceId, String jobName, BigInteger peId, Integer portIndex) {
		boolean matches = matchesDomainId(domainId) && (_instanceFilters.size() > 0);
		if (matches) {
			for(InstanceFilter filter : _instanceFilters.values()) {
				matches = filter.matchesPeInputPortIndex(instanceId, jobName, peId, portIndex);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeInputPortMetricName(String domainId, String instanceId, String jobName, BigInteger peId, Integer portIndex, String metricName) {
		boolean matches = matchesDomainId(domainId) && (_instanceFilters.size() > 0);
		if (matches) {
			for(InstanceFilter filter : _instanceFilters.values()) {
				matches = filter.matchesPeInputPortMetricName(instanceId, jobName, peId, portIndex, metricName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeOutputPortIndex(String domainId, String instanceId, String jobName, BigInteger peId, Integer portIndex) {
		boolean matches = matchesDomainId(domainId) && (_instanceFilters.size() > 0);
		if (matches) {
			for(InstanceFilter filter : _instanceFilters.values()) {
				matches = filter.matchesPeOutputPortIndex(instanceId, jobName, peId, portIndex);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeOutputPortMetricName(String domainId, String instanceId, String jobName, BigInteger peId, Integer portIndex, String metricName) {
		boolean matches = matchesDomainId(domainId) && (_instanceFilters.size() > 0);
		if (matches) {
			for(InstanceFilter filter : _instanceFilters.values()) {
				matches = filter.matchesPeOutputPortMetricName(instanceId, jobName, peId, portIndex, metricName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}
	
	public boolean matchesPeConnectionId(String domainId, String instanceId, String jobName, BigInteger peId, String connectionId) {
		boolean matches = matchesDomainId(domainId) && (_instanceFilters.size() > 0);
		if (matches) {
			for(InstanceFilter filter : _instanceFilters.values()) {
				matches = filter.matchesPeConnectionId(instanceId, jobName, peId, connectionId);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}
	
	public boolean matchesPeConnectionMetricName(String domainId, String instanceId, String jobName, BigInteger peId, String connectionId, String metricName) {
		boolean matches = matchesDomainId(domainId) && (_instanceFilters.size() > 0);
		if (matches) {
			for(InstanceFilter filter : _instanceFilters.values()) {
				matches = filter.matchesPeConnectionMetricName(instanceId, jobName, peId, connectionId, metricName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

}
