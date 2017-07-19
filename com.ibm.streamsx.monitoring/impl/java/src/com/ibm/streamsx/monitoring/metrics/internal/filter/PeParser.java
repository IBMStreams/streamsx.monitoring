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

import com.ibm.json.java.JSONArtifact;
import com.ibm.json.java.JSONObject;

public class PeParser extends AbstractParser {
	
	private static Logger _logger = Logger.getLogger(PeParser.class.getName());

	private static final String METRIC_NAME_PATTERNS = "metricNamePatterns";

	private static final String INPUT_PORTS = "inputPorts";

	private static final String OUTPUT_PORTS = "outputPorts";
	
	private static final String CONNECTIONS = "connections";

	private PortParser _portParser = new PortParser();
	
	private ConnectionParser _connectionParser = new ConnectionParser();
	
	protected PeParser() {

		setValidationRule(METRIC_NAME_PATTERNS, new IValidator() {

			@Override
			public boolean validate(String key, Object object) {
				return verifyPatterns(key, object);
			}
			
		});

		setValidationRule(INPUT_PORTS, new IValidator() {

			@Override
			public boolean validate(String key, Object object) {
				boolean result = true;
				if (object instanceof JSONArtifact) {
					result = _portParser.validate((JSONArtifact)object);
				}
				else {
					result = false;
					logger().error("filterDocument: The parsed object must be a JSONArtifact. Details: key=" + key + ", object=" + object);
				}
				return result;
			}
			
		});

		setValidationRule(OUTPUT_PORTS, new IValidator() {

			@Override
			public boolean validate(String key, Object object) {
				boolean result = true;
				if (object instanceof JSONArtifact) {
					result = _portParser.validate((JSONArtifact)object);
				}
				else {
					result = false;
					logger().error("filterDocument: The parsed object must be a JSONArtifact. Details: key=" + key + ", object=" + object);
				}
				return result;
			}
			
		});
		
		setValidationRule(CONNECTIONS, new IValidator() {

			@Override
			public boolean validate(String key, Object object) {
				boolean result = true;
				if (object instanceof JSONArtifact) {
					result = _connectionParser.validate((JSONArtifact)object);
				}
				else {
					result = false;
					logger().error("filterDocument: The parsed object must be a JSONArtifact. Details: key=" + key + ", object=" + object);
				}
				return result;
			}
			
		});
	}

	@Override
	protected Logger logger() {
		return _logger;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Set<PeFilter> buildFilters(JSONObject json) {
//		logger().error("PE.JSON=" + json);
		Set<String> metrics = buildPatternList(json.get(METRIC_NAME_PATTERNS));
		Set<PortFilter> inputPortFilters = _portParser.buildFilters((JSONArtifact)json.get(INPUT_PORTS));
		Set<PortFilter> outputPortFilters = _portParser.buildFilters((JSONArtifact)json.get(OUTPUT_PORTS));
		Set<ConnectionFilter> connectionFilters = _connectionParser.buildFilters((JSONArtifact)json.get(CONNECTIONS));
		Set<MetricFilter> metricFilters = new HashSet<>();
		for (String pattern : metrics) {
//			logger().error("create metric filter, pattern=" + pattern);
			metricFilters.add(new MetricFilter(pattern));
		}
		Set<PeFilter> result = new HashSet<>();
//		logger().error("create PE filter");
		result.add(new PeFilter(metricFilters, inputPortFilters, outputPortFilters, connectionFilters));
		return result;
	}

}
