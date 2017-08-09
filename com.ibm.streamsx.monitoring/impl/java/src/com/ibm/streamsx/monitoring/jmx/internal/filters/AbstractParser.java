//
// ****************************************************************************
// * Copyright (C) 2016, 2017, International Business Machines Corporation    *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.jmx.internal.filters;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONArtifact;
import com.ibm.json.java.JSONObject;

abstract class AbstractParser {
	
	private Set<String> _mandatoryItems = new HashSet<>();

	private Map<String, IValidator> _itemValidators = new HashMap<>();

	protected abstract Logger logger();

	protected void setMandatoryItem(String key) {
		_mandatoryItems.add(key);
	}
	
	protected void setValidationRule(String key, IValidator validator) {
		_itemValidators.put(key, validator);
	}
	
	protected boolean validate(JSONArtifact json) {
		boolean result = true;
		if (json instanceof JSONArray) {
			for (Object obj : (JSONArray)json) {
				if (obj instanceof JSONObject) {
					result &= validate((JSONObject)obj);
				}
			}
		}
		else if (json instanceof JSONObject) {
			result &= validate((JSONObject)json);
		}
		return result;
	}

	protected boolean validate(JSONObject json) {
		boolean result = true;
		// Do the mandatory items exist?
		for (String key : _mandatoryItems) {
			if (json.containsKey(key) == false) {
				result = false;
				logger().error("filterDocument: A mandatory key is missing. Details: key=" + key + ", json=" + json);
			}
		}
		// Run the validators.
		for (Object key : json.keySet()) {
			if (key instanceof String) {
				if (_itemValidators.containsKey(key)) {
					if (_itemValidators.get(key).validate((String)key, json.get(key)) == false) {
						result = false;
						logger().error("filterDocument: The validation fails for a key. Details: key=" + key + ", json=" + json.get(key));
					}
				}
				else {
					logger().error("filterDocument: There is an unexpected key. Details: key=" + key + ", json=" + json);
				}
			}
			else {
				logger().error("filterDocument: The key is not a string. Details: key=" + key + ", json=" + json);
			}
		}
		return result;
		
	}

	protected boolean verifyPatterns(String key, Object json) {
		boolean result = true;
		if (json instanceof JSONArray) {
			for (Object obj : (JSONArray)json) {
				if (obj instanceof String) {
					result &= isValidPattern(key, (String)obj);
				}
				else {
					result = false;
					logger().error("filterDocument: Pattern object must be a string. Details: key=" + key + ", object=" + obj);
				}
			}
		}
		else if (json instanceof String) {
			result &= isValidPattern(key, (String)json);
		}
		else {
			result = false;
			logger().error("filterDocument: Pattern object must be a string. Details: key=" + key + ", object=" + json);
		}
		return result;
	}

	protected boolean isValidPattern(String key, String pattern) {
		boolean result = true;
		try {
			Pattern.compile(pattern);
		}
		catch(PatternSyntaxException e) {
			result = false;
			logger().error("filterDocument: Invalid pattern. Details: key=" + key + ", pattern=" + pattern);
		}
		return result;
	}
	
	protected boolean verifyNumbers(String key, Object json) {
		boolean result = true;
		if (json instanceof JSONArray) {
			for (Object obj : (JSONArray)json) {
				if (obj instanceof String) {
					result &= isValidNumber(key, (String)obj);
				}
				else {
					result = false;
					logger().error("filterDocument: Number object must be a string. Details: key=" + key + ", object=" + obj);
				}
			}
		}
		else if (json instanceof String) {
			result &= isValidNumber(key, (String)json);
		}
		else {
			result = false;
			logger().error("filterDocument: Number object must be a string. Details: key=" + key + ", object=" + json);
		}
		return result;
	}

	protected boolean isValidNumber(String key, String value) {
		boolean result = true;
		try {
			if (value.equals("*") == false) {
				Long number = Long.valueOf(value);
				if (number < 0) {
					result = false;
					logger().error("filterDocument: Negative number. Details: key=" + key + ", value=" + value);
				}
			}
		}
		catch(NumberFormatException e) {
			result = false;
			logger().error("filterDocument: Invalid number. Details: key=" + key + ", value=" + value);
		}
		return result;
	}
	
	protected Set<String> buildPatternList(Object json) {
		Set<String> result = new HashSet<>();
		if (json instanceof JSONArray) {
			for (Object obj : (JSONArray)json) {
				if (obj instanceof String) {
					result.add((String)obj);
				}
			}
		}
		else if (json instanceof String) {
			result.add((String)json);
		}
		return result;
	}
	
	protected Set<Long> buildNumberList(Object json) {
		Set<Long> result = new HashSet<>();
		if (json instanceof JSONArray) {
			for (Object obj : (JSONArray)json) {
				String value = (String)obj; // instanceof is already verified
				if (value.equals("*") == false) {
					result.add(Long.valueOf(value));
				}
				else {
					result.add(NumberMatcher.EVERY_NUMBER);
				}
			}
		}
		else if (json instanceof String) {
			String value = (String)json; // instanceof is already verified
			if (value.equals("*") == false) {
				result.add(Long.valueOf(value));
			}
			else {
				result.add(NumberMatcher.EVERY_NUMBER);
			}
		}
		return result;
	}
	
	protected <T> Set<T> buildFilters(JSONArtifact json) {
//		logger().error("JSON=" + json);
		Set<T> filters = new HashSet<>();
		if (json instanceof JSONArray) {
			for (Object obj : (JSONArray)json) {
				if (obj instanceof JSONObject) {
					Set<T> tmp = buildFilters((JSONObject)obj);
					if (tmp != null) {
						filters.addAll(tmp);
					}
				}
			}
		}
		else if (json instanceof JSONObject) {
			Set<T> tmp = buildFilters((JSONObject)json);
			if (tmp != null) {
				filters.addAll(tmp);
			}
		}
		return filters;
	}

	protected abstract <T> Set<T> buildFilters(JSONObject json);

	protected Set<Long> buildNumberList(String key, Object json) {
		Set<Long> result = new HashSet<>();
		if (json instanceof JSONArray) {
			for (Object obj : (JSONArray)json) {
				if (obj instanceof String) {
					if (((String)obj).equals("*")) {
						result.add(null);
					}
					else {
						result.add(Long.valueOf((String)obj));
					}
				}
			}
		}
		else if (json instanceof String) {
			if (((String)json).equals("*")) {
				result.add(null);
			}
			else {
				result.add(Long.valueOf((String)json));
			}
		}
		return result;
	}
	
}
