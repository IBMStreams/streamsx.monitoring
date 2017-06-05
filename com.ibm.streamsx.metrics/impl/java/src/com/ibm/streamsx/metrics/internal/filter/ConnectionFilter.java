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

public class ConnectionFilter extends PatternMatcher {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(ConnectionFilter.class.getName());

	/**
	 * A port has many metrics.
	 */
	protected Map<String /* regular expression */, MetricFilter> _metricFilters = new HashMap<>();

	public ConnectionFilter(String connectionId, Set<MetricFilter> metricFilters) throws PatternSyntaxException {
		super(connectionId);
		for(MetricFilter metricFilter : metricFilters) {
			_metricFilters.put(metricFilter.getRegularExpression(), metricFilter);
		}
	}

	public boolean matchesConnectionId(String connectionId) {
		boolean matches = matches(connectionId);
		return matches;
	}

	public boolean matchesConnectionMetricName(String connectionId, String metricName) {
		boolean matches = matchesConnectionId(connectionId) && (_metricFilters.size() > 0);
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

}
