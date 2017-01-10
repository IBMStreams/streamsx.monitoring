//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.metrics.internal.filter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

final class OperatorFilter extends PatternMatcher implements Filter {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(OperatorFilter.class.getName());

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

	public OperatorFilter(String regularExpression, Set<Filter> metricFilters, Set<Filter> inputPortFilters, Set<Filter> outputPortFilters) throws PatternSyntaxException {
		super(regularExpression);
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

	public boolean matchesOperatorName(String operatorName) {
		boolean matches = matches(operatorName);
		return matches;
	}

	public boolean matchesOperatorMetricName(String operatorName, String metricName) {
		boolean matches = matchesOperatorName(operatorName) && (_metricFilters.size() > 0);
		if (matches) {
			for(MetricFilter filter : _metricFilters.values()) {
				matches = filter.matchesMetricName(metricName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesOperatorInputPortIndex(String operatorName, Integer portIndex) {
		boolean matches = matchesOperatorName(operatorName) && (_inputPortFilters.size() > 0);
		if (matches) {
			for(PortFilter filter : _inputPortFilters.values()) {
				matches = filter.matchesPortIndex(portIndex);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesOperatorOutputPortIndex(String operatorName, Integer portIndex) {
		boolean matches = matchesOperatorName(operatorName) && (_outputPortFilters.size() > 0);
		if (matches) {
			for(PortFilter filter : _outputPortFilters.values()) {
				matches = filter.matchesPortIndex(portIndex);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

}
