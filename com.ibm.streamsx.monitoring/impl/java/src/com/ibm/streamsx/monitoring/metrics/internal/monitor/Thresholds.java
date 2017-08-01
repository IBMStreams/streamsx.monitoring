//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.metrics.internal.monitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.Tuple;

/**
 * Thresholds is a container for the thresholds inside a threshold rule. 
 * There can be many thresholds defined inside a threshold rule (eg. value, 
 * rollingAverage, rate, etc.).
 */
public class Thresholds {
	
	/**
	 * Threshold types.
	 */
	public static final String VALUE = "value", ROLLING_AVG = "rollingAverage", INCREASE_PERCENTAGE = "increasePercentage", RATE = "rate";
	public static final String[] THRESHOLD_TYPES = new String[] { VALUE, ROLLING_AVG, INCREASE_PERCENTAGE, RATE };
	
	/**
	 * Map linking threshold types to threshold values.
	 */
	protected Map<String /*thresholdType*/, Threshold /*thresholdValue*/> thresholds = new HashMap<String /*thresholdType*/, Threshold /*threshold*/>();
	
	/**
	 * Store new threshold.
	 * 
	 */
	public void putThreshold(String thresholdType, Threshold threshold) {
		thresholds.put(thresholdType, threshold);
	}
	
	/**
	 * Check if defined thresholds have been violated. If so, create and submit alerts 
	 * for those thresholds.
	 */
	public void checkAndSubmitAlerts(List<Tuple> metricTuples, long parsedTime, OperatorContext context) {
		for (String thresholdType : THRESHOLD_TYPES) {
			if (thresholds.containsKey(thresholdType) && thresholds.get(thresholdType).alerted()) {
				continue;
			}
			
			if (thresholds.containsKey(thresholdType)) {
				double metricValue = (double)metricTuples.get(metricTuples.size() - 1).getLong(Metrics.METRIC_VALUE);
				Integer timeFrame = thresholds.get(thresholdType).getTimeFrame();
				
				// Only check threshold rules if threshold timeframe has elapsed.
				if (timeFrame != null && thresholdTimeFrameHasElapsed(thresholdType, parsedTime)) {
					List<Tuple> filteredTuplesWithinTimeFrame = getTuplesWithinTimeFrame(metricTuples, timeFrame);
					if (filteredTuplesWithinTimeFrame.size() > 1) {
						metricValue = Metrics.calculatedValue(filteredTuplesWithinTimeFrame, thresholdType, timeFrame.intValue());
					}
				}
				
				// If threshold is reached, submit alert.
				if (thresholdValueReached(thresholdType, metricValue)) {
					Threshold threshold = thresholds.get(thresholdType);
					Alert alert = new Alert(metricTuples.get(0), metricValue, threshold);
					alert.submitAlert(context);
					thresholds.get(thresholdType).setAlerted(true);
				}
			}
		}
	}
	
	/**
	 * Get tuples within given timeframe.
	 * 
	 * @return
	 * Returns a list of tuples within the given timeframe.
	 */
	public List<Tuple> getTuplesWithinTimeFrame(List<Tuple> metricTuples, int timeFrame) {
		List<Tuple> filteredTuples = new ArrayList<Tuple>();
		long smallestTimeToInclude = System.currentTimeMillis() - timeFrame;
		
		for (Tuple metricTuple : metricTuples) {
			long metricRetrievalTime = metricTuple.getLong(Metrics.LAST_TIME_RETRIEVED);
			
			if (metricRetrievalTime > smallestTimeToInclude) {
				filteredTuples.add(metricTuple);
			}
		}
		
		return filteredTuples;
	}

	/**
	 * Check whether threshold value has been reached, given the current metric value.
	 * 
	 * @return
	 * Returns true if threshold value has been reached.
	 */
	protected Boolean thresholdValueReached(String thresholdType, double currentValue) {
		if (thresholds.containsKey(thresholdType)) {
			Double thresholdValueObject = thresholds.get(thresholdType).getValue();
			
			if (thresholdValueObject != null) {
				double thresholdValue = thresholdValueObject.doubleValue();
				String operator = thresholds.get(thresholdType).getOperator();
				
				if (operator == null && currentValue == thresholdValue) {
					return true;
				} else if (operator != null) {
					if (operator.equals(">=") && currentValue >= thresholdValue) {
						return true;
					} else if (operator.equals("<=") && currentValue <= thresholdValue) {
						return true;
					} else if (operator.equals(">") && currentValue > thresholdValue) {
						return true;
					} else if (operator.equals("<") && currentValue < thresholdValue) {
						return true;
					}
				}
			}
		}
		
		return false;
	}
	
	/**
	 * Check whether threshold's timeframe has elapsed. Only start monitoring when 
	 * threshold's timeframe has elapsed (prevents using data before thresholds 
	 * were loaded).
	 * 
	 * @return
	 * Returns true if threshold's timeframe has elapsed.
	 */
	protected Boolean thresholdTimeFrameHasElapsed(String thresholdType, long thresholdsLoadedTime) {
		if (thresholds.containsKey(thresholdType)) {
			long timeSinceThresholdsLoaded = System.currentTimeMillis() - thresholdsLoadedTime;
			long thresholdTimeFrame = thresholds.get(thresholdType).getTimeFrame().longValue();
			
			if(timeSinceThresholdsLoaded > thresholdTimeFrame) {
				return true;
			}
		}
		
		return false;
	}
	
	/**
	 * Reset all threshold alerts.
	 */
	public void resetAlerts() {
		for (String key : thresholds.keySet()) {
			thresholds.get(key).setAlerted(false);
		}
	}
}
