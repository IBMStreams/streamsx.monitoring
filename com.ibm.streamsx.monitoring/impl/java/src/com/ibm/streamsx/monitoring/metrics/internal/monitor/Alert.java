//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.metrics.internal.monitor;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;

public class Alert {
	
	private String message;
	
	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(Alert.class.getName());
	
	/**
	 * Initialize Alert.
	 */
	public Alert(Tuple tuple, Double currentValue, Threshold threshold) {
		
		// Get name and unit.
		String thresholdName = tuple.getString(Metrics.METRIC_NAME);
		Double thresholdValue = threshold.getValue();
		String thresholdType = threshold.getType();
		String thresholdOperator = threshold.getOperator();
		Integer timeFrame = threshold.getTimeFrame();
		String unit = getUnit(thresholdType);
		
		// Format operator.
		thresholdOperator = (thresholdOperator != null) ? thresholdOperator : "";
		
		// Construct alert message.
		message = thresholdType + " of " + thresholdName + " reached!\n"
					   + "You specified a " + thresholdType + " of " + thresholdOperator + String.valueOf(thresholdValue) + unit + ". "
					   + "The current " + thresholdType + " is " + String.valueOf(currentValue) + unit + ". \n";	
		
		// Add time-frame information.
		addTimeFrameInformation(timeFrame);
		
		// Add additional metrics information to alert message.
		addMetricsInformation(tuple);
	}
	
	/**
	 * Get unit for given threshold type.
	 * 
	 * @return
	 * Returns the unit for the given threshold type.
	 */
	private String getUnit(String thresholdType) {
		if (thresholdType.equals(Thresholds.VALUE) || thresholdType.equals(Thresholds.ROLLING_AVG)) {
			return "";
		} else if (thresholdType.equals(Thresholds.INCREASE_PERCENTAGE)) {
			return "%";
		} else {
			return "/s";
		}
	}
	
	/**
	 * Add timeframe information to message.
	 */
	private void addTimeFrameInformation(Integer timeFrame) {
		if (timeFrame != null) {
			message = message.replaceAll("\\. ", " in the past " + timeFrame/1000 + "s. ");
		}
	}
	
	/**
	 * Add metrics information to message.
	 */
	private void addMetricsInformation(Tuple tuple) {
		message = message + "Metrics Information: [ ";
		
		for (String metricType : Metrics.METRIC_TYPES) {
			try {
				String metricProperty = String.valueOf(tuple.getObject(metricType));
				message = message + metricType + "=" + metricProperty + ", ";
			} catch (NullPointerException e) {
				// Filter type does not exist. Safe to ignore.
			}
		}
		
		message = message.substring(0, message.length() - 2) + " ]";
	}
	
	public void submitAlert(OperatorContext context) {
		StreamingOutput<OutputTuple> streamingOutput = context.getStreamingOutputs().get(0);
		
		// Submit tuple with message attribute.
		try {
			OutputTuple outputTuple = streamingOutput.newTuple();
			outputTuple.setString("message", message);
			streamingOutput.submit(outputTuple);
		} catch (Exception e) {
			_trace.error("Exception submitting alert tuple.", e);
		}
	}
}
