package com.ibm.streamsx.metrics.internal.filter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

final class PeFilter implements Filter {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(PeFilter.class.getName());

	/**
	 * An operator has many metrics.
	 */
	protected Map<String /* regular expression */, MetricFilter> _metricFilters = new HashMap<>();

	/**
	 * An operator has many input ports.
	 */
	protected Map<Long /* port index  */, PortFilter> _inputPortFilters = new HashMap<>();

	/**
	 * An operator has many output ports.
	 */
	protected Map<Long /* port index */, PortFilter> _outputPortFilters = new HashMap<>();

	public PeFilter(Set<Filter> metricFilters, Set<Filter> inputPortFilters, Set<Filter> outputPortFilters) throws PatternSyntaxException {
		for(Filter filter : metricFilters) {
			MetricFilter metricFilter = (MetricFilter)filter;
			_metricFilters.put(metricFilter.getRegularExpression(), metricFilter);
		}
		for(Filter filter : inputPortFilters) {
			PortFilter portFilter = (PortFilter)filter;
			_inputPortFilters.put(portFilter.getNumber(), portFilter);
		}
		for(Filter filter : outputPortFilters) {
			PortFilter portFilter = (PortFilter)filter;
			_outputPortFilters.put(portFilter.getNumber(), portFilter);
		}
	}

	public boolean matchesPeMetricName(String metricName) {
		boolean matches = false;
		for(MetricFilter filter : _metricFilters.values()) {
			matches = filter.matchesMetricName(metricName);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesPeInputPortIndex(Integer portIndex) {
		boolean matches = false;
		for(PortFilter filter : _inputPortFilters.values()) {
			matches = filter.matchesPortIndex(portIndex);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesPeOutputPortIndex(Integer portIndex) {
		boolean matches = false;
		for(PortFilter filter : _outputPortFilters.values()) {
			matches = filter.matchesPortIndex(portIndex);
			if (matches) {
				break;
			}
		}
		return matches;
	}

}
