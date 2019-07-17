//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.metrics.internal.monitor;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.streams.operator.Tuple;

/**
 * Filters is a container for the filters inside a ThresholdObject. 
 * There can be many filters defined inside a ThresholdObject (e.g.  
 * instanceId, jobName, etc.).
 */
public class Filters {
	
	/**
	 * Map linking filter types to filter values.
	 */
	private Map<String /*filterType*/, String /*filterValue*/> filters = new HashMap<String /*filterType*/, String /*filterValue*/>();

	/**
	 * Store new filter.
	 */
	public void putFilter(String filterType, String filterValue) {
		filters.put(filterType, filterValue);
	}
	
    /**
     * Applies stored filters to given stored metrics and returns filtered stored metrics.
     * 
     * @return
     * Returns the given stored metrics, after filter is applied.
     */
    public LinkedList<Tuple> getFilteredTuples(List<Tuple> storedMetricTuples) {
		LinkedList<Tuple> filteredMetricTuples = new LinkedList<Tuple>(storedMetricTuples);
		LinkedList<Tuple> removeList = new LinkedList<Tuple>();

    	for (Tuple tuple : filteredMetricTuples) {
    		for (String filterType : Metrics.METRIC_TYPES) {
    			if (filters.containsKey(filterType)) {
    				String filterRegex = filters.get(filterType);
    				String metricTupleProperty = tuple.getString(filterType);
					Matcher matcher = Pattern.compile(filterRegex).matcher(metricTupleProperty);
					if (!matcher.matches()) {
						removeList.add(tuple);
					}
    			}
    		}
    	}

    	filteredMetricTuples.removeAll(removeList);
		return filteredMetricTuples;
    }
}
