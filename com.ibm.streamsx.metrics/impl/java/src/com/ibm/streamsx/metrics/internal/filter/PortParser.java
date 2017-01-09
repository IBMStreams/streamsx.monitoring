package com.ibm.streamsx.metrics.internal.filter;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.ibm.json.java.JSONArtifact;
import com.ibm.json.java.JSONObject;

public class PortParser extends AbstractParser {
	
	private static Logger _logger = Logger.getLogger(PortParser.class.getName());

	private static final String PORT_INDEXES = "portIndexes";

	private static final String METRIC_NAME_PATTERNS = "metricNamePatterns";

	protected PortParser() {

		setMandatoryItem(METRIC_NAME_PATTERNS);

		setValidationRule(PORT_INDEXES, new IValidator() {

			@Override
			public boolean validate(String key, Object object) {
				return verifyNumbers(key, object);
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

	@Override
	protected Set<Filter> buildFilters(JSONObject json) {
		logger().error("PortParser.JSON=" + json);
		Set<Long> indexes = buildNumberList(json.get(PORT_INDEXES));
		Set<String> metrics = buildPatternList(json.get(METRIC_NAME_PATTERNS));
		Set<Filter> metricFilters = new HashSet<>();
		for (String pattern : metrics) {
			logger().error("create metric filter, pattern=" + pattern);
			metricFilters.add(new MetricFilter(pattern));
		}
		Set<Filter> result = new HashSet<>();
		for (Long index : indexes) {
			logger().error("create port filter, index=" + index);
			result.add(new PortFilter(index, metricFilters));
		}
		return result;
	}

}
