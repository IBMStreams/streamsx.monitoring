//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.metrics.internal.monitor;

/**
 * A single Threshold defined inside a ThresholdObject.
 */
public class Threshold {
	
	/**
	 * Threshold's type.
	 */
	private String type;
	
	/**
	 * Threshold's value.
	 */
	private Double value;

	/**
	 * Threshold's timeframe (in milliseconds).
	 */
	private Integer timeFrame;
	
	/**
	 * Threshold's operator ("<", ">", "<=", ">=").
	 */
	private String operator;
	
	/**
	 * Threshold's alerted status.
	 */
	private Boolean alerted;
	
	/**
	 * Initialize Threshold.
	 */
	public Threshold(String type, Double value, Integer timeFrame, String operator) {
		this.type = type;
		this.value = value;
		this.timeFrame = timeFrame;
		this.operator = operator;
		alerted = false;
	}

	public Double getValue() {
		return value;
	}
	
	public String getType() {
		return type;
	}
	
	public Integer getTimeFrame() {
		return timeFrame;
	}
	
	public String getOperator() {
		return operator;
	}
	
	/**
	 * Check whether threshold has been alerted.
	 * 
	 * @return
	 * Returns true if threshold has been alerted.
	 */
	public Boolean alerted() {
		return alerted;
	}
	
	/**
	 * Set threshold's alerted status.
	 */
	public void setAlerted(Boolean alerted) {
		this.alerted = alerted;
	}

}
