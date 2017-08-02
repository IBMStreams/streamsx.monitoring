//
// ****************************************************************************
// * Copyright (C) 2016, 2017, International Business Machines Corporation    *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.jobs.internal.filter;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

import org.apache.log4j.Logger;

final class JobFilter extends PatternMatcher {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(JobFilter.class.getName());

	/**
	 * A job has many operators.
	 */
	protected Map<String /* regular expression */, OperatorFilter> _operatorFilters = new HashMap<>();

	/**
	 * A job has many PEs.
	 */
	protected Set<PeFilter> _peFilters = new HashSet<>();

	public JobFilter(String regularExpression, Set<OperatorFilter> operatorFilters, Set<PeFilter> peFilters) throws PatternSyntaxException {
		super(regularExpression);
		for(OperatorFilter operatorFilter : operatorFilters) {
			_operatorFilters.put(operatorFilter.getRegularExpression(), operatorFilter);
		}
		for(PeFilter peFilter : peFilters) {
			_peFilters.add(peFilter);
		}
	}

	public boolean matchesJobName(String jobName) {
		boolean matches = matches(jobName);
		return matches;
	}

	public boolean matchesOperatorName(String jobName, String operatorName) {
		boolean matches = matchesJobName(jobName) && (_operatorFilters.size() > 0);
		if (matches) {
			for(OperatorFilter filter : _operatorFilters.values()) {
				matches = filter.matchesOperatorName(operatorName);
				if (matches) {
					break;
				}
			}
		}
		return matches;
	}

	public boolean matchesPeId(String jobName, BigInteger peId) {
		boolean matches = matchesJobName(jobName);
		return matches;
	}


}
