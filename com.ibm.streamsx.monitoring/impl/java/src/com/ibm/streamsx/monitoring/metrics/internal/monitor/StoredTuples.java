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
import java.util.Queue;

import com.ibm.streams.operator.Tuple;

/**
 * Container for stored metric tuples.
 */
public class StoredTuples {
	
	/**
	 * Stored metric Tuples.
	 */
	private Map<String /*metricName*/, Queue<Tuple>> storedMetricTuples = new HashMap<String, Queue<Tuple>>();
	
	/**
	 * The currently monitored metric.
	 */
	private Tuple currentlyMonitoredMetric;

	/**
	 * Get all Tuples with matching metric name.
	 * 
	 * @return
	 * Returns a list of Tuples with matching metric name.
	 */
	public List<Tuple> getMatchingTuples(String metricName) {
		if (!storedMetricTuples.containsKey(metricName)) {
			storedMetricTuples.put(metricName, new LinkedList<Tuple>());
		}
		
		return new LinkedList<Tuple>(storedMetricTuples.get(metricName));
	}
	
	/**
	 * Get the currently monitored metric Tuple.
	 * 
	 * @return
	 * Returns the currently monitored metric Tuple.
	 */
	public Tuple getMonitoredMetric() {
		return currentlyMonitoredMetric;
	}
	
	/**
	 * Store a new Tuple.
	 */
	public void add(Tuple tuple) {
		String metricName = tuple.getString(Metrics.METRIC_NAME);
		
		if(!storedMetricTuples.containsKey(metricName)) {
			storedMetricTuples.put(metricName, new LinkedList<Tuple>());
    	}
		
		storedMetricTuples.get(metricName).add(tuple);
		currentlyMonitoredMetric = tuple;
	}
	
	/**
	 * Remove a given amount of Tuples for given metric name, 
	 * from oldest to newest.
	 */
	public void remove(String metricName, int amount) {
		while (amount > 0) {
			storedMetricTuples.get(metricName).remove();
    		--amount;
    	}
	}
	
	/**
	 * Get the amount of Tuples stored for the given metric name.
	 * 
	 * @return
	 * Returns the amount of Tuples stored the the given metric name.
	 */
	public int getSize(String metricName) {
		return storedMetricTuples.get(metricName).size();
	}
	
}
