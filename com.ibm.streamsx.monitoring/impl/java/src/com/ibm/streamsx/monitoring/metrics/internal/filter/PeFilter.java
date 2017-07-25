//
// ****************************************************************************
// * Copyright (C) 2016, 2017, International Business Machines Corporation    *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.metrics.internal.filter;

import java.math.BigInteger;
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
	
	/**
	 * A PE has many connections.
	 */
	protected Map<String /* connection id */, ConnectionFilter> _connectionFilters = new HashMap<>();

	public PeFilter(Set<MetricFilter> metricFilters, Set<PortFilter> inputPortFilters, Set<PortFilter> outputPortFilters, Set<ConnectionFilter> connectionFilters) throws PatternSyntaxException {
		for(MetricFilter metricFilter : metricFilters) {
			_metricFilters.put(metricFilter.getRegularExpression(), metricFilter);
		}
		for(PortFilter portFilter : inputPortFilters) {
			_inputPortFilters.put(portFilter.getNumber(), portFilter);
		}
		for(PortFilter portFilter : outputPortFilters) {
			_outputPortFilters.put(portFilter.getNumber(), portFilter);
		}
		for(ConnectionFilter connectionFilter : connectionFilters) {
			_connectionFilters.put(connectionFilter.getRegularExpression(), connectionFilter);
		}
	}

	public boolean matchesPeMetricName(BigInteger peId, String metricName) {
		boolean matches = false;
		for(MetricFilter filter : _metricFilters.values()) {
			matches = filter.matchesMetricName(metricName);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesPeInputPortIndex(BigInteger peId, Integer portIndex) {
		boolean matches = false;
		for(PortFilter filter : _inputPortFilters.values()) {
			matches = filter.matchesPortIndex(portIndex);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesPeInputPortMetricName(BigInteger peId, Integer portIndex, String metricName) {
		boolean matches = false;
		for(PortFilter filter : _inputPortFilters.values()) {
			matches = filter.matchesPortMetricName(portIndex, metricName);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesPeOutputPortIndex(BigInteger peId, Integer portIndex) {
		boolean matches = false;
		for(PortFilter filter : _outputPortFilters.values()) {
			matches = filter.matchesPortIndex(portIndex);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesPeOutputPortMetricName(BigInteger peId, Integer portIndex, String metricName) {
		boolean matches = false;
		for(PortFilter filter : _outputPortFilters.values()) {
			matches = filter.matchesPortMetricName(portIndex, metricName);
			if (matches) {
				break;
			}
		}
		return matches;
	}
	
	public boolean matchesPeConnectionId(BigInteger peId, String connectionId) {
		boolean matches = false;
		for(ConnectionFilter filter : _connectionFilters.values()) {
			matches = filter.matchesConnectionId(connectionId);
			if (matches) {
				break;
			}
		}
		return matches;
	}
	
	public boolean matchesPeConnectionMetricName(BigInteger peId, String peConnection, String metricName) {
		boolean matches = false;
		for(ConnectionFilter filter : _connectionFilters.values()) {
			matches = filter.matchesConnectionMetricName(peConnection, metricName);
			if (matches) {
				break;
			}
		}
		return matches;
	}

}
