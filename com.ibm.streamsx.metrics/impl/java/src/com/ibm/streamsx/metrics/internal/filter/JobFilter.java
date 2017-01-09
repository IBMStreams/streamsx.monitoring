package com.ibm.streamsx.metrics.internal.filter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

final class JobFilter extends PatternMatcher implements Filter {

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

	public JobFilter(String regularExpression, Set<Filter> operatorFilters, Set<Filter> peFilters) throws PatternSyntaxException {
		super(regularExpression);
		for(Filter filter : operatorFilters) {
			OperatorFilter operatorFilter = (OperatorFilter)filter;
			_operatorFilters.put(operatorFilter.getRegularExpression(), operatorFilter);
		}
		for(Filter filter : peFilters) {
			PeFilter peFilter = (PeFilter)filter;
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

	public boolean matchesPeMetricName(String jobName, String metricName) {
		_trace.error("matchesPeMetricName(j=" + jobName + ",m=" + metricName + "): size=" + _peFilters.size());
		boolean matches = matchesJobName(jobName) && (_peFilters.size() > 0);
		if (matches) {
			for(PeFilter filter : _peFilters) {
				matches = filter.matchesPeMetricName(metricName);
				if (matches) {
					break;
				}
			}
		}
		_trace.error("matchesPeMetricName(j=" + jobName + ",m=" + metricName + "): " + Boolean.toString(matches));
		return matches;
	}

	public boolean matchesPeInputPortIndex(String jobName, Integer portIndex) {
		boolean matches = matchesJobName(jobName) && (_peFilters.size() > 0);
		if (matches) {
			for(PeFilter filter : _peFilters) {
				matches = filter.matchesPeInputPortIndex(portIndex);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeOutputPortIndex(String jobName, Integer portIndex) {
		boolean matches = matchesJobName(jobName) && (_peFilters.size() > 0);
		if (matches) {
			for(PeFilter filter : _peFilters) {
				matches = filter.matchesPeOutputPortIndex(portIndex);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

}
