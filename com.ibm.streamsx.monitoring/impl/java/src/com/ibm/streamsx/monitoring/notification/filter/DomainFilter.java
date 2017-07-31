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

final class DomainFilter extends PatternMatcher {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(DomainFilter.class.getName());

	/**
	 * A domain has many instances.
	 */
	protected Map<String /* regular expression */, InstanceFilter> _instanceFilters = new HashMap<>();

	public DomainFilter(String regularExpression, Set<InstanceFilter> filters) throws PatternSyntaxException {
		super(regularExpression);
		for(InstanceFilter instanceFilter : filters) {
			_instanceFilters.put(instanceFilter.getRegularExpression(), instanceFilter);
		}
	}

	public boolean matchesDomainId(String domainId) {
		boolean matches = matches(domainId);
		return matches;
	}

	public boolean matchesInstanceId(String domainId, String instanceId) {
		boolean matches = matchesDomainId(domainId) && (_instanceFilters.size() > 0);
		if (matches) {
			for(InstanceFilter filter : _instanceFilters.values()) {
				matches = filter.matchesInstanceId(instanceId);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesJobName(String domainId, String instanceId, String jobName) {
		boolean matches = matchesDomainId(domainId) && (_instanceFilters.size() > 0);
		if (matches) {
			for(InstanceFilter filter : _instanceFilters.values()) {
				matches = filter.matchesJobName(instanceId, jobName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesOperatorName(String domainId, String instanceId, String jobName, String operatorName) {
		boolean matches = matchesDomainId(domainId) && (_instanceFilters.size() > 0);
		if (matches) {
			for(InstanceFilter filter : _instanceFilters.values()) {
				matches = filter.matchesOperatorName(instanceId, jobName, operatorName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeId(String domainId, String instanceId, String jobName, BigInteger peId) {
		boolean matches = matchesDomainId(domainId) && (_instanceFilters.size() > 0);
		if (matches) {
			for(InstanceFilter filter : _instanceFilters.values()) {
				matches = filter.matchesPeId(instanceId, jobName, peId);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

}
