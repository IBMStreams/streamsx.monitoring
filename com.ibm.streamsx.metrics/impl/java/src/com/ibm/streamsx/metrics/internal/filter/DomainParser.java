//
// ****************************************************************************
// * Copyright (C) 2016, 2017, International Business Machines Corporation    *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.metrics.internal.filter;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.ibm.json.java.JSONArtifact;
import com.ibm.json.java.JSONObject;

public class DomainParser extends AbstractParser {
	
	private static Logger _logger = Logger.getLogger(DomainParser.class.getName());

	private static final String DOMAIN_ID_PATTERNS = "domainIdPatterns";
	
	private static final String INSTANCES = "instances";
	
	private InstanceParser _instanceParser = new InstanceParser();
	
	protected DomainParser() {

		setMandatoryItem(DOMAIN_ID_PATTERNS);
		setMandatoryItem(INSTANCES);

		setValidationRule(DOMAIN_ID_PATTERNS, new IValidator() {

			@Override
			public boolean validate(String key, Object object) {
				return verifyPatterns(key, object);
			}
			
		});

		setValidationRule(INSTANCES, new IValidator() {

			@Override
			public boolean validate(String key, Object object) {
				boolean result = true;
				if (object instanceof JSONArtifact) {
					result = _instanceParser.validate((JSONArtifact)object);
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
	protected Set<DomainFilter> buildFilters(JSONObject json) {
//		logger().error("Domain.JSON=" + json);
		Set<String> patterns = buildPatternList(json.get(DOMAIN_ID_PATTERNS));
		Set<InstanceFilter> filters = _instanceParser.buildFilters((JSONArtifact)json.get(INSTANCES));
		Set<DomainFilter> result = new HashSet<>();
		for (String pattern : patterns) {
//			logger().error("create domain filter, pattern=" + pattern);
			result.add(new DomainFilter(pattern, filters));
		}
		return result;
	}

}
