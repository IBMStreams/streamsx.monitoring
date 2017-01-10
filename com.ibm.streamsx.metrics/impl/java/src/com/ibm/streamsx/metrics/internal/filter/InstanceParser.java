//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.metrics.internal.filter;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.ibm.json.java.JSONArtifact;
import com.ibm.json.java.JSONObject;

public class InstanceParser extends AbstractParser {
	
	private static Logger _logger = Logger.getLogger(InstanceParser.class.getName());

	private static final String INSTANCE_ID_PATTERNS = "instanceIdPatterns";
	
	private static final String JOBS = "jobs";

	private JobParser _jobParser = new JobParser();
	
	protected InstanceParser() {

		setMandatoryItem(INSTANCE_ID_PATTERNS);
		setMandatoryItem(JOBS);

		setValidationRule(INSTANCE_ID_PATTERNS, new IValidator() {

			@Override
			public boolean validate(String key, Object object) {
				return verifyPatterns(key, object);
			}
			
		});

		setValidationRule(JOBS, new IValidator() {

			@Override
			public boolean validate(String key, Object object) {
				boolean result = true;
				if (object instanceof JSONArtifact) {
					result = _jobParser.validate((JSONArtifact)object);
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
//		logger().error("Instance.JSON=" + json);
		Set<String> patterns = buildPatternList(json.get(INSTANCE_ID_PATTERNS));
		Set<Filter> filters = _jobParser.buildFilters((JSONArtifact)json.get(JOBS));
		Set<Filter> result = new HashSet<>();
		for (String pattern : patterns) {
//			logger().error("create instance filter, pattern=" + pattern);
			result.add(new InstanceFilter(pattern, filters));
		}
		return result;
	}

}
