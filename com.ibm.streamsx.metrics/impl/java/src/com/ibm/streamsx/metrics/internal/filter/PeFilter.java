//
// ****************************************************************************
// * Copyright (C) 2016, 2017, International Business Machines Corporation    *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.metrics.internal.filter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

final class PeFilter {

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

	public PeFilter(Set<MetricFilter> metricFilters, Set<PortFilter> inputPortFilters, Set<PortFilter> outputPortFilters) throws PatternSyntaxException {
		for(MetricFilter metricFilter : metricFilters) {
			_metricFilters.put(metricFilter.getRegularExpression(), metricFilter);
		}
		for(PortFilter portFilter : inputPortFilters) {
			_inputPortFilters.put(portFilter.getNumber(), portFilter);
		}
		for(PortFilter portFilter : outputPortFilters) {
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

	public boolean matchesPeInputPortMetricName(Integer portIndex, String metricName) {
		boolean matches = false;
		for(PortFilter filter : _inputPortFilters.values()) {
			matches = filter.matchesPortMetricName(portIndex, metricName);
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

	public boolean matchesPeOutputPortMetricName(Integer portIndex, String metricName) {
		boolean matches = false;
		for(PortFilter filter : _outputPortFilters.values()) {
			matches = filter.matchesPortMetricName(portIndex, metricName);
			if (matches) {
				break;
			}
		}
		return matches;
	}

}
