package com.ibm.streamsx.metrics.internal.filter;

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

	private PortParser _portParser = new PortParser();
	
	protected OperatorParser() {

		setMandatoryItem(OPERATOR_NAME_PATTERNS);

		setValidationRule(OPERATOR_NAME_PATTERNS, new IValidator() {

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

	@Override
	protected Logger logger() {
		return _logger;
	}

	@Override
	protected Set<Filter> buildFilters(JSONObject json) {
		logger().error("Operator.JSON=" + json);
		Set<String> patterns = buildPatternList(json.get(OPERATOR_NAME_PATTERNS));
		Set<String> metrics = buildPatternList(json.get(METRIC_NAME_PATTERNS));
		Set<Filter> inputPortFilters = _portParser.buildFilters((JSONArtifact)json.get(INPUT_PORTS));
		Set<Filter> outputPortFilters = _portParser.buildFilters((JSONArtifact)json.get(OUTPUT_PORTS));
		Set<Filter> metricFilters = new HashSet<>();
		for (String pattern : metrics) {
			logger().error("create metric filter, pattern=" + pattern);
			metricFilters.add(new MetricFilter(pattern));
		}
		Set<Filter> result = new HashSet<>();
		for (String pattern : patterns) {
			logger().error("create operator filter, pattern=" + pattern);
			result.add(new OperatorFilter(pattern, metricFilters, inputPortFilters, outputPortFilters));
		}
		return result;
	}

}
