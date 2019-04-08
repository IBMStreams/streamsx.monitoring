//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.metrics.internal.monitor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.ibm.streams.operator.Tuple;

/**
 * Metric types and functions for calculating rolling average, increase percentage, 
 * and rate.
 */
public class Metrics {
	
	/**
	 * Metric types.
	 */
	public static final String METRIC_NAME = "metricName", METRIC_VALUE = "metricValue", LAST_TIME_RETRIEVED = "lastTimeRetrieved";
	
	public static final String[] METRIC_TYPES = new String[] { "instanceId", "jobId", "jobName", "resource", "peId", "operatorName", 
															   "channel", "portIndex", "connectionId", "metricType", "metricKind", "metricName", 
															   "metricValue", "lastTimeRetrieved"};

	public static final Set<String> METRIC_SCHEMA = new HashSet<String>(Arrays.asList(METRIC_TYPES));
	
	/**
	 * Get calculated value, given calculation type.
	 * 
	 * @return
	 * Returns calculated value, given calculation type.
	 */
	public static Double calculatedValue(List<Tuple> tuples, String calculationType, int timeFrame) {
		switch (calculationType) {
			case Thresholds.ROLLING_AVG:
				return calculateRollingAverage(tuples);
			case Thresholds.INCREASE_PERCENTAGE:
				return calculateIncreasePercentage(tuples);
			case Thresholds.RATE:
				return calculateRate(tuples, timeFrame);
		}
		
		return null;
	}
	
	/**
	 * Calculate rolling average, given metric tuples.
	 * 
	 * @return
	 * Returns rolling average for given metric tuples.
	 */
	private static Double calculateRollingAverage(List<Tuple> tuples) {
		double sum = 0, numHits = 0;
		
		for (Tuple tuple : tuples) {
			double newValue = (double)tuple.getLong(Metrics.METRIC_VALUE);
			
			// Track sum of metric values to calculate average at the end.
			sum += newValue;
			numHits++;
		}
		
		Double rollingAverage = Double.valueOf(sum/numHits);
		return rollingAverage;
	}
	
	/**
	 * Calculate increase percentage, given metric tuples.
	 * 
	 * @return
	 * Returns increase percentage for given metric tuples.
	 */
	private static Double calculateIncreasePercentage(List<Tuple> tuples) {
		double currentValue = 0, increases = 0, decreases = 0;
		
		for (Tuple tuple : tuples) {
	    	double nextValue = (double)tuple.getLong(Metrics.METRIC_VALUE);
	    	
	    	// Track increases and decreases for attribute.
        	if(currentValue < nextValue) {
        		increases++;
        	} else {
        		decreases++;
        	}
        	
        	currentValue = nextValue;
    	}
		
		Double currentIncreasePercentage = Double.valueOf((increases/(increases + decreases))*100);
		return currentIncreasePercentage;
	}
	
	/**
	 * Calculate rate, given metric tuples and timeframe.
	 * 
	 * @return
	 * Return rate for given metric tuples.
	 */
	private static Double calculateRate(List<Tuple> tuples, int timeFrame) {
		long earliestTime = Long.MAX_VALUE, latestTime = Long.MIN_VALUE; 
		double firstValue = 0, lastValue = 0;
		
		for (Tuple tuple : tuples) {
				long currentHitTime = tuple.getLong(Metrics.LAST_TIME_RETRIEVED);
		    	
		    	// Track first and last metric values.
	        	if (earliestTime > currentHitTime) {
	        		earliestTime = currentHitTime;
	        		firstValue = (double)tuple.getLong(Metrics.METRIC_VALUE);
	        	} 
	        	
	        	if (latestTime < currentHitTime) {
	        		latestTime = currentHitTime;
	        		lastValue = (double)tuple.getLong(Metrics.METRIC_VALUE);
	        	}
    	}
		
		Double rate = Double.valueOf((lastValue - firstValue) / (timeFrame/1000));
		return rate;
	}
	
}
