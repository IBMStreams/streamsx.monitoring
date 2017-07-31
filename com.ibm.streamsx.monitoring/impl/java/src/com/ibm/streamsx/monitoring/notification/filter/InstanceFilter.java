//
// ****************************************************************************
// * Copyright (C) 2016, 2017, International Business Machines Corporation    *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.notification.filter;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

final class InstanceFilter extends PatternMatcher {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(InstanceFilter.class.getName());

	/**
	 * An instance has many jobs.
	 */
	protected Map<String /* regular expression */, JobFilter> _jobFilters = new HashMap<>();

	public InstanceFilter(String regularExpression, Set<JobFilter> filters) throws PatternSyntaxException {
		super(regularExpression);
		for(JobFilter jobFilter : filters) {
			_jobFilters.put(jobFilter.getRegularExpression(), jobFilter);
		}
	}

	public boolean matchesInstanceId(String instanceId) {
		boolean matches = matches(instanceId);
		return matches;
	}

	public boolean matchesJobName(String instanceId, String jobName) {
		boolean matches = matchesInstanceId(instanceId) && (_jobFilters.size() > 0);
		if (matches) {
			for(JobFilter filter : _jobFilters.values()) {
				matches = filter.matchesJobName(jobName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesOperatorName(String instanceId, String jobName, String operatorName) {
		boolean matches = matchesInstanceId(instanceId) && (_jobFilters.size() > 0);
		if (matches) {
			for(JobFilter filter : _jobFilters.values()) {
				matches = filter.matchesOperatorName(jobName, operatorName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeId(String instanceId, String jobName, BigInteger peId) {
		boolean matches = matchesInstanceId(instanceId) && (_jobFilters.size() > 0);
		if (matches) {
			for(JobFilter filter : _jobFilters.values()) {
				matches = filter.matchesPeId(jobName, peId);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

}
