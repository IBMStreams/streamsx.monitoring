package com.ibm.streamsx.metrics.internal.filter;

import java.util.Set;
import java.util.regex.PatternSyntaxException;

final class PEFilter implements Filter {

	public PEFilter(Set<Filter> filters) throws PatternSyntaxException {
//		super("");
//		for(Filter filter : filters) {
//			addFilter(filter);
//		}
	}

//	public boolean matches(String operatorName, String metricName, int stopLevel) {
//		boolean matches = (operatorName != null) && _matcher.reset(operatorName).matches();
//		if (matches) {
//			--stopLevel;
//			if (stopLevel > 0) {
//				for(Filter filter : _filters.values()) {
//					matches = ((MetricFilter)filter).matches(metricName, stopLevel);
//					if (matches) {
//						break;
//					}
//				}
//			}
//		}
//		return matches;
//	}

}
