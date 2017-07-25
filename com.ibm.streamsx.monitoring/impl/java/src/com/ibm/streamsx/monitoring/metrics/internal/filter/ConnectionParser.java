//
// ****************************************************************************
// * Copyright (C) 2016, 2017, International Business Machines Corporation    *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.metrics.internal.filter;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.ibm.json.java.JSONObject;

public class ConnectionParser extends AbstractParser {
	private static Logger _logger = Logger.getLogger(PortParser.class.getName());

	private static final String CONNECTION_ID_PATTERNS = "connectionIdPatterns";

	private static final String METRIC_NAME_PATTERNS = "metricNamePatterns";

	protected ConnectionParser() {

		setMandatoryItem(METRIC_NAME_PATTERNS);

		setValidationRule(CONNECTION_ID_PATTERNS, new IValidator() {

			@Override
			public boolean validate(String key, Object object) {
				return verifyPatterns(key, object);
			}
			
		});

		setValidationRule(METRIC_NAME_PATTERNS, new IValidator() {

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
	protected Set<ConnectionFilter> buildFilters(JSONObject json) {
//		logger().error("ConnectionParser.JSON=" + json);
		Set<String> patterns = buildPatternList(json.get(CONNECTION_ID_PATTERNS));
		Set<String> metrics = buildPatternList(json.get(METRIC_NAME_PATTERNS));
		Set<MetricFilter> metricFilters = new HashSet<>();
		for (String pattern : metrics) {
//			logger().error("create metric filter, pattern=" + pattern);
			metricFilters.add(new MetricFilter(pattern));
		}
		Set<ConnectionFilter> result = new HashSet<>();
		for (String pattern : patterns) {
//			logger().error("create connection filter, connectionId=" + pattern);
			result.add(new ConnectionFilter(pattern, metricFilters));
		}
		return result;
	}

}
