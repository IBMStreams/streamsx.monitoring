//
// ****************************************************************************
// * Copyright (C) 2016, 2017, International Business Machines Corporation    *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.notification.filter;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.ibm.json.java.JSONArtifact;
import com.ibm.json.java.JSONObject;

public class OperatorParser extends AbstractParser {
	
	private static Logger _logger = Logger.getLogger(OperatorParser.class.getName());

	private static final String OPERATOR_NAME_PATTERNS = "operatorNamePatterns";
	
	private static final String METRIC_NAME_PATTERNS = "metricNamePatterns";

	private static final String INPUT_PORTS = "inputPorts";

	private static final String OUTPUT_PORTS = "outputPorts";
	
	protected OperatorParser() {

		setMandatoryItem(OPERATOR_NAME_PATTERNS);

		setValidationRule(OPERATOR_NAME_PATTERNS, new IValidator() {

			@Override
			public boolean validate(String key, Object object) {
				return verifyPatterns(key, object);
			}
			
		});

	}

	@Override
	protected Logger logger() {
		return _logger;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Set<OperatorFilter> buildFilters(JSONObject json) {
//		logger().error("Operator.JSON=" + json);
		Set<String> patterns = buildPatternList(json.get(OPERATOR_NAME_PATTERNS));
		Set<OperatorFilter> result = new HashSet<>();
		for (String pattern : patterns) {
//			logger().error("create operator filter, pattern=" + pattern);
			result.add(new OperatorFilter(pattern));
		}
		return result;
	}

}
