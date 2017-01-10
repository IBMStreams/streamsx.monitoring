//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.metrics.internal.filter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

/**
 * The 
 */
class PatternMatcher {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(PatternMatcher.class.getName());

	/**
	 * 
	 */
	private String _regularExpression = null;
	
	/**
	 * 
	 */
	private Pattern _pattern = null;
	
	/**
	 * 
	 */
	private Matcher _matcher = null;
	
	/**
	 * Construct a filter.
	 * 
	 * @param regularExpression
	 * Specifies the regular expression for this filter.
	 * 
	 * @throws PatternSyntaxException
	 * Throws this exception if the regular expression cannot be compiled.
	 */
	protected PatternMatcher(String regularExpression) throws PatternSyntaxException {
		_regularExpression = regularExpression;
		_pattern = Pattern.compile(regularExpression);
		_matcher = _pattern.matcher("");
	}

	/**
	 * Get the regular expression with which the class is instantiated.
	 * 
	 * @return The regular expression of this filter.
	 */
	protected String getRegularExpression() {
		return _regularExpression;
	}

	/**
	 * 
	 */
	protected boolean matches(String value) {
		boolean matches = (value != null) && _matcher.reset(value).matches();
		if (_trace.isInfoEnabled()) {
			_trace.info(String.format("matches(%s): %s -> %s", value, _regularExpression, Boolean.toString(matches)));
		}
		return matches;
	}
	
}
