package com.ibm.streamsx.metrics.internal.filter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

final class PortFilter extends NumberMatcher implements Filter {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(PortFilter.class.getName());

	/**
	 * A port has many metrics.
	 */
	protected Map<String /* regular expression */, MetricFilter> _metricFilters = new HashMap<>();

	public PortFilter(Long portIndex, Set<Filter> metricFilters) throws PatternSyntaxException {
		super(portIndex);
		for(Filter filter : metricFilters) {
			MetricFilter metricFilter = (MetricFilter)filter;
			_metricFilters.put(metricFilter.getRegularExpression(), metricFilter);
		}
	}

	public boolean matchesPortIndex(Integer portIndex) {
		boolean matches = matches(portIndex);
		return matches;
	}

	public boolean matchesPortMetricName(Integer portIndex, String metricName) {
		boolean matches = matchesPortIndex(portIndex) && (_metricFilters.size() > 0);
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
