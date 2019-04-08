//
// ****************************************************************************
// * Copyright (C) 2016, 2017, International Business Machines Corporation    *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.jmx.internal.filters;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

import com.ibm.streamsx.monitoring.jmx.OperatorConfiguration.OpType;

final class JobFilter extends PatternMatcher {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(JobFilter.class.getName());

	/**
	 * A job has many operators.
	 */
	protected Map<String /* regular expression */, OperatorFilter> _operatorFilters = new HashMap<>();

	/**
	 * A job has many PEs.
	 */
	protected Set<PeFilter> _peFilters = new HashSet<>();
	
	private OpType _type;

	public JobFilter(String regularExpression, Set<OperatorFilter> operatorFilters, Set<PeFilter> peFilters, OpType aType) throws PatternSyntaxException {
		super(regularExpression);
		_type = aType;
		if (_type == OpType.METRICS_SOURCE) {
			for(OperatorFilter operatorFilter : operatorFilters) {
				_operatorFilters.put(operatorFilter.getRegularExpression(), operatorFilter);
			}
		}
		for(PeFilter peFilter : peFilters) {
			_peFilters.add(peFilter);
		}
	}

	public boolean matchesJobName(String jobName) {
		boolean matches = matches(jobName);
		return matches;
	}

	public boolean matchesOperatorName(String jobName, String operatorName) {
		boolean matches = matchesJobName(jobName) && (_operatorFilters.size() > 0);
		if (matches) {
			for(OperatorFilter filter : _operatorFilters.values()) {
				matches = filter.matchesOperatorName(operatorName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesOperatorMetricName(String jobName, String operatorName, String metricName) {
		boolean matches = matchesJobName(jobName) && (_operatorFilters.size() > 0);
		if (matches) {
			for(OperatorFilter filter : _operatorFilters.values()) {
				matches = filter.matchesOperatorMetricName(operatorName, metricName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesOperatorInputPortIndex(String jobName, String operatorName, Integer portIndex) {
		boolean matches = matchesJobName(jobName) && (_operatorFilters.size() > 0);
		if (matches) {
			for(OperatorFilter filter : _operatorFilters.values()) {
				matches = filter.matchesOperatorInputPortIndex(operatorName, portIndex);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesOperatorInputPortMetricName(String jobName, String operatorName, Integer portIndex, String metricName) {
		boolean matches = matchesJobName(jobName) && (_operatorFilters.size() > 0);
		if (matches) {
			for(OperatorFilter filter : _operatorFilters.values()) {
				matches = filter.matchesOperatorInputPortMetricName(operatorName, portIndex, metricName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesOperatorOutputPortIndex(String jobName, String operatorName, Integer portIndex) {
		boolean matches = matchesJobName(jobName) && (_operatorFilters.size() > 0);
		if (matches) {
			for(OperatorFilter filter : _operatorFilters.values()) {
				matches = filter.matchesOperatorOutputPortIndex(operatorName, portIndex);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesOperatorOutputPortMetricName(String jobName, String operatorName, Integer portIndex, String metricName) {
		boolean matches = matchesJobName(jobName) && (_operatorFilters.size() > 0);
		if (matches) {
			for(OperatorFilter filter : _operatorFilters.values()) {
				matches = filter.matchesOperatorOutputPortMetricName(operatorName, portIndex, metricName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeId(String jobName, String peId) {
		boolean matches = false;
		if (_type == OpType.METRICS_SOURCE) {
			matches = matchesJobName(jobName) && (_peFilters.size() > 0);
		}
		else if (_type == OpType.JOB_STATUS_SOURCE) {
			matches = matchesJobName(jobName);
		}
		return matches;
	}

	public boolean matchesPeMetricName(String jobName, String peId, String metricName) {
		boolean matches = matchesPeId(jobName, peId);
		if (matches) {
			for(PeFilter filter : _peFilters) {
				matches = filter.matchesPeMetricName(peId, metricName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeInputPortIndex(String jobName, String peId, Integer portIndex) {
		boolean matches = matchesPeId(jobName, peId);
		if (matches) {
			for(PeFilter filter : _peFilters) {
				matches = filter.matchesPeInputPortIndex(peId, portIndex);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeInputPortMetricName(String jobName, String peId, Integer portIndex, String metricName) {
		boolean matches = matchesPeId(jobName, peId);
		if (matches) {
			for(PeFilter filter : _peFilters) {
				matches = filter.matchesPeInputPortMetricName(peId, portIndex, metricName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeOutputPortIndex(String jobName, String peId, Integer portIndex) {
		boolean matches = matchesPeId(jobName, peId);
		if (matches) {
			for(PeFilter filter : _peFilters) {
				matches = filter.matchesPeOutputPortIndex(peId, portIndex);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeOutputPortMetricName(String jobName, String peId, Integer portIndex, String metricName) {
		boolean matches = matchesPeId(jobName, peId);
		if (matches) {
			for(PeFilter filter : _peFilters) {
				matches = filter.matchesPeOutputPortMetricName(peId, portIndex, metricName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}
	
	public boolean matchesPeConnectionId(String jobName, String peId, String connectionId) {
		boolean matches = matchesPeId(jobName, peId);
		if (matches) {
			for(PeFilter filter : _peFilters) {
				matches = filter.matchesPeConnectionId(peId, connectionId);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}
	
	public boolean matchesPeConnectionMetricName(String jobName, String peId, String connectionId, String metricName) {
		boolean matches = matchesPeId(jobName, peId);
		if (matches) {
			for(PeFilter filter : _peFilters) {
				matches = filter.matchesPeConnectionMetricName(peId, connectionId, metricName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

}
