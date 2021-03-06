//
// ****************************************************************************
// * Copyright (C) 2016, 2017, International Business Machines Corporation    *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.jmx.internal.filters;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.ibm.json.java.JSONArtifact;
import com.ibm.json.java.JSONObject;
import com.ibm.streamsx.monitoring.jmx.OperatorConfiguration.OpType;
import com.ibm.streamsx.monitoring.jmx.internal.filters.OperatorFilter;

public class OperatorParser extends AbstractParser {
	
	private static Logger _logger = Logger.getLogger(OperatorParser.class.getName());

	private static final String OPERATOR_NAME_PATTERNS = "operatorNamePatterns";
	
	private static final String METRIC_NAME_PATTERNS = "metricNamePatterns";

	private static final String INPUT_PORTS = "inputPorts";

	private static final String OUTPUT_PORTS = "outputPorts";

	private PortParser _portParser;

	private OpType _type;
	
	protected OperatorParser(OpType aType) {
		_type = aType;
		if (_type == OpType.METRICS_SOURCE) {
			_portParser = new PortParser();
		}
		setMandatoryItem(OPERATOR_NAME_PATTERNS);

		setValidationRule(OPERATOR_NAME_PATTERNS, new IValidator() {

			@Override
			public boolean validate(String key, Object object) {
				return verifyPatterns(key, object);
			}
			
		});

		if (_type == OpType.METRICS_SOURCE) {
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
		}
	}

	@Override
	protected Logger logger() {
		return _logger;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Set<OperatorFilter> buildFilters(JSONObject json) {
//		logger().error("Operator.JSON=" + json);
		if (_type == OpType.METRICS_SOURCE) {
			Set<String> patterns = buildPatternList(json.get(OPERATOR_NAME_PATTERNS));
			Set<String> metrics = buildPatternList(json.get(METRIC_NAME_PATTERNS));
			Set<PortFilter> inputPortFilters = _portParser.buildFilters((JSONArtifact)json.get(INPUT_PORTS));
			Set<PortFilter> outputPortFilters = _portParser.buildFilters((JSONArtifact)json.get(OUTPUT_PORTS));
			Set<MetricFilter> metricFilters = new HashSet<>();
			for (String pattern : metrics) {
	//			logger().error("create metric filter, pattern=" + pattern);
				metricFilters.add(new MetricFilter(pattern));
			}
			Set<OperatorFilter> result = new HashSet<>();
			for (String pattern : patterns) {
	//			logger().error("create operator filter, pattern=" + pattern);
				result.add(new OperatorFilter(pattern, metricFilters, inputPortFilters, outputPortFilters));
			}
			return result;
		}
		else {
			Set<String> patterns = buildPatternList(json.get(OPERATOR_NAME_PATTERNS));
			Set<OperatorFilter> result = new HashSet<>();
			for (String pattern : patterns) {
				result.add(new OperatorFilter(pattern));
			}
			return result;			
		}
	}

}
