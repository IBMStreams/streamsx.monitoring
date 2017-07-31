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

public class PeParser extends AbstractParser {
	
	private static Logger _logger = Logger.getLogger(PeParser.class.getName());

	
	protected PeParser() {

	}

	@Override
	protected Logger logger() {
		return _logger;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected Set<PeFilter> buildFilters(JSONObject json) {
//		logger().error("PE.JSON=" + json);
		Set<PeFilter> result = new HashSet<>();
//		logger().error("create PE filter");
		result.add(new PeFilter());
		return result;
	}

}
