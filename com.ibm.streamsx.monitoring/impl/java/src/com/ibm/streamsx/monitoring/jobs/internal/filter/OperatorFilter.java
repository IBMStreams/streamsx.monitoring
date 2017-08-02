//
// ****************************************************************************
// * Copyright (C) 2016, 2017, International Business Machines Corporation    *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.jobs.internal.filter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

final class OperatorFilter extends PatternMatcher {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(OperatorFilter.class.getName());

	public OperatorFilter(String regularExpression) throws PatternSyntaxException {
		super(regularExpression);
	}

	public boolean matchesOperatorName(String operatorName) {
		boolean matches = matches(operatorName);
		return matches;
	}

}
