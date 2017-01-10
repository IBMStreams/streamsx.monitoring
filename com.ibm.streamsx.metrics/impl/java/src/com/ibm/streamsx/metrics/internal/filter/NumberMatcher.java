//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.metrics.internal.filter;

import org.apache.log4j.Logger;

/**
 * The 
 */
class NumberMatcher {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(NumberMatcher.class.getName());

	/**
	 * Port numbers are greater or equal to zero. A -1 means, every port.
	 */
	protected static Long EVERY_NUMBER = -1l;
	
	/**
	 * 
	 */
	private Long _number = null;
	
	/**
	 * Construct a filter.
	 * 
	 * @param number
	 * Specifies the number for this filter.
	 */
	protected NumberMatcher(Long number) {
		_number = number;
	}

	/**
	 * Get the regular expression with which the class is instantiated.
	 * 
	 * @return The regular expression of this filter.
	 */
	protected Long getNumber() {
		return _number;
	}

	/**
	 * 
	 */
	protected boolean matches(Integer portIndex) {
		boolean matches = (portIndex != null) && (_number.equals(EVERY_NUMBER) || portIndex.equals(_number));
		if (_trace.isInfoEnabled()) {
			_trace.info(String.format("matches(%s): %d -> %s", portIndex, _number, Boolean.toString(matches)));
		}
		return matches;
	}
	
}
