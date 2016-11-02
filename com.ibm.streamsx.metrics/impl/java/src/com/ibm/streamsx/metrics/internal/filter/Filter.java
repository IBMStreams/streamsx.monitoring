package com.ibm.streamsx.metrics.internal.filter;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * The 
 */
abstract class Filter {

	/**
	 * 
	 */
	protected String _regularExpression = null;
	
	/**
	 * 
	 */
	protected Pattern _pattern = null;
	
	/**
	 * 
	 */
	protected Matcher _matcher = null;
	
	/**
	 * 
	 */
	protected Map<String /* regular expression */, Filter> _filters = new HashMap<String,Filter>();
	
	/**
	 * Construct a filter.
	 * 
	 * @param regularExpression
	 * Specifies the regular expression for this filter.
	 * 
	 * @throws PatternSyntaxException
	 * Throws this exception if the regular expression cannot be compiled.
	 */
	public Filter(String regularExpression) throws PatternSyntaxException {
		_regularExpression = regularExpression;
		_pattern = Pattern.compile(regularExpression);
		_matcher = _pattern.matcher("");
	}

	/**
	 * Add a sub filter.
	 * 
	 * @param filter Specifies the sub filter.
	 */
	public void addFilter(Filter filter) {
		_filters.put(filter.getRegularExpression(), filter);
	}

	/**
	 * Get the regular expression with which the class is instantiated.
	 * 
	 * @return The regular expression of this filter.
	 */
	public String getRegularExpression() {
		return _regularExpression;
	}

}
