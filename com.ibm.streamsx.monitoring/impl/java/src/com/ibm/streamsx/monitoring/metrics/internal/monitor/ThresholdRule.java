//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.metrics.internal.monitor;

import java.io.IOException;
import java.util.List;

import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.Tuple;

/**
 * A ThresholdRule contains a metric name, thresholds, and filters.
 */
public class ThresholdRule {
	
	/**
	 * Key for the thresholds object in the thresholdDocument JSON.
	 */
	private static final String THRESHOLDS = "thresholds";

	/**
	 * Key for the filters object in the thresholdDocument JSON.
	 */
	private static final String FILTERS = "filters";
	
	/**
	 * Threshold rule's metric name.
	 */
	private String metricName;
	
	/**
	 * Threshold rule's thresholds.
	 */
	private Thresholds thresholds;
	
	/**
	 * Threshold rule's filters.
	 */
	private Filters filters;
	
	/**
	 * Initialize the ThresholdRule.
	 * 
	 * @throws IOException 
	 * Throws IOException if the thresholds object exists but is not in the correct 
	 * format (JSONObject).
	 */
	public ThresholdRule(JSONObject jsonObject) throws IOException {
		metricName = extractMetricName(jsonObject);
		thresholds = extractThresholds(jsonObject);
		filters = extractFilters(jsonObject);
	}
	
	/**
	 * Extract metric name from JSONObject.
	 * 
	 * @return
	 * Returns extract metric name.
	 * 
	 * @throws IOException
	 * Throws an IOException if metric name exists but is not in the form of a String.
	 */
	private String extractMetricName(JSONObject jsonObject) throws IOException {
		Object metricName = getObject(jsonObject, Metrics.METRIC_NAME);
		
		if (metricName == null) {
			throw new IOException("metricName must be defined in thresholdDocument: " + jsonObject.toString());
		} else if (metricName != null && !(metricName instanceof String)) {
			throw new IOException("metricName must be in the form of String in thresholdDocument: " + jsonObject.toString());
		}
		
		return (String)metricName;
	}

	/**
	 * Extract thresholds from a JSONObject.
	 * 
	 * @return
	 * Returns the extracted thresholds.
	 * 
	 * @throws IOException
	 * Throws an IOException if thresholds property exists but is not in the form 
	 * of a JSONObject.
	 */
	private Thresholds extractThresholds(JSONObject jsonObject) throws IOException {
		Thresholds extractedThresholds = new Thresholds();
		Object thresholdsObject = getObject(jsonObject, THRESHOLDS);
		
		if (thresholdsObject != null && thresholdsObject instanceof JSONObject) {
			for (String thresholdType : Thresholds.THRESHOLD_TYPES) {
				Object thresholdObject = getObject((JSONObject)thresholdsObject, thresholdType);
				
				if(thresholdObject != null && thresholdObject instanceof String) {
					String thresholdValueString = ((String)thresholdObject).split(",")[0];
					
					// Extract operator.
					String thresholdOperator = extractThresholdOperator(thresholdValueString);
					while(!Character.isDigit(thresholdValueString.charAt(0))) {
						thresholdValueString = thresholdValueString.substring(1);
					}

					// Parse remaining value to Double.
					Double thresholdValue = Double.parseDouble(thresholdValueString);
					
					// Get timeframe, if it exists.
					Integer thresholdTimeFrame = null;
					if (((String)thresholdObject).split(",").length == 2) {
						String thresholdTimeFrameString = ((String)thresholdObject).split(",")[1];
						thresholdTimeFrame = Integer.parseInt(thresholdTimeFrameString);
					}
					
					Threshold threshold = new Threshold(thresholdType, thresholdValue, thresholdTimeFrame, thresholdOperator);
					extractedThresholds.putThreshold(thresholdType, threshold);
					
				} else if (thresholdObject != null && !(thresholdObject instanceof String)) {
					throw new IOException("'" + thresholdType + "' must be in the form of a String: " + thresholdsObject.toString());
				}
			}
		} else if (thresholdsObject != null && !(thresholdsObject instanceof JSONObject)) {
			throw new IOException(THRESHOLDS + " must be in the form of a JSONObject: " + thresholdsObject.toString());
		}
		
		return extractedThresholds;
	}
	
	/**
	 * Extract filters from a JSONObject.
	 * 
	 * @return
	 * Returns the extracted filters.
	 * 
	 * @throws IOException
	 * Throws an IOException if filters property exists but is not in the form 
	 * of a JSONObject.
	 */
	private Filters extractFilters(JSONObject jsonObject) throws IOException {
		Filters extractedFilters = new Filters();
		Object filtersObject = getObject(jsonObject, FILTERS);
		
		if (filtersObject != null && filtersObject instanceof JSONObject) {
			for (String filterType : Metrics.METRIC_TYPES) {
				String filterRegex = (String)getObject((JSONObject)filtersObject, filterType);
				
				if (filterRegex != null) {
					extractedFilters.putFilter(filterType, filterRegex);
				}
			}
		} else if (filtersObject != null && !(filtersObject instanceof JSONObject)){
			throw new IOException(FILTERS + " must be in the form of a JSONObject: " + filtersObject.toString());
		}
		
		return extractedFilters;
	}
	
	/**
	 * Extract operator from a given String.
	 * 
	 * @return
	 * Returns the extracted operator.
	 */
	private String extractThresholdOperator(String value) {
		if (value.startsWith(">=")) {
			return ">=";
		} else if (value.startsWith("<=")) {
			return "<=";
		} else if (value.startsWith(">")) {
			return ">";
		} else if (value.startsWith("<")) {
			return "<";
		}
		
		return null;
	}
	
	/**
	 * Get Object from a JSONObject, given the key. There are many optional properties 
	 * that can be defined in the thresholdDocument.
	 * 
	 * Trying to retrieve non-existent ones lead to IOExceptions being thrown. This 
	 * function simply returns null if properties are not found.
	 * 
	 * @return
	 * Returns an Object from the given JSONObject.
	 * 
	 */
	private Object getObject(JSONObject jsonObject, String key) {
		if (jsonObject.containsKey(key)) {
			return jsonObject.get(key);
		}
		
		return null;
	}
	
	public String getMetricName() {
		return metricName;
	}
	
	/**
	 * Filter stored tuples with filters inside ThresholdRule. Then, check and submit alerts.
	 */
	public void checkAndSubmitAlerts(List<Tuple> storedMetricTuples, long parsedTime, OperatorContext context) {
		List<Tuple> filteredMetricTuples = filters.getFilteredTuples(storedMetricTuples);
		thresholds.checkAndSubmitAlerts(filteredMetricTuples, parsedTime, context);
	}
	
	/**
	 * Reset all threshold alerts.
	 */
	public void resetAlerts() {
		thresholds.resetAlerts();
	}
}
