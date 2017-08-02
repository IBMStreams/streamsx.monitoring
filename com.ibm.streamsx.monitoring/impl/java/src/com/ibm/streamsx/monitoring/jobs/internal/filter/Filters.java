//
// ****************************************************************************
// * Copyright (C) 2016, 2017, International Business Machines Corporation    *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.jobs.internal.filter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.ibm.json.java.JSON;
import com.ibm.json.java.JSONArtifact;

/**
 * 
 */
public class Filters {

	/**
	 * 
	 */
	protected Map<String /* regular expression */, DomainFilter> _domainFilters = new HashMap<>();

	public Filters() {
	}

	public boolean matchesDomainId(String domainId) {
		boolean matches = false;
		for(DomainFilter filter : _domainFilters.values()) {
			matches = filter.matchesDomainId(domainId);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesInstanceId(String domainId, String instanceId) {
		boolean matches = false;
		for(DomainFilter filter : _domainFilters.values()) {
			matches = filter.matchesInstanceId(domainId, instanceId);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesJobName(String domainId, String instanceId, String jobName) {
		boolean matches = false;
		for(DomainFilter filter : _domainFilters.values()) {
			matches = filter.matchesJobName(domainId, instanceId, jobName);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesOperatorName(String domainId, String instanceId, String jobName, String operatorName) {
		boolean matches = false;
		for(DomainFilter filter : _domainFilters.values()) {
			matches = filter.matchesOperatorName(domainId, instanceId, jobName, operatorName);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesPeId(String domainId, String instanceId, String jobName, BigInteger peId) {
		boolean matches = false;
		for(DomainFilter filter : _domainFilters.values()) {
			matches = filter.matchesPeId(domainId, instanceId, jobName, peId);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	/**
	 * Read the filterDocument file, parse its JSON-formatted content, and
	 * build a filter tree.
	 * 
	 * @param filterDocument
	 * Specifies the absolute path of a JSON-formatted file, which contains
	 * the specification of the filter criteria for domain, instance, job,
	 * operator names.
	 * 
	 * @return
	 * A tree of filter objects that is used to evaluate whether a given
	 * domain, instance, job, operator name matches the specified
	 * filter criteria.
	 * 
	 * @throws IOException
	 * This exception is thrown under the following conditions.
	 * <ul>
	 * <li>An I/O error occurred, for example, the filterDocument file does not exist.</li>
	 * <li>The filterDocument file contains an invalid JSON document.</li>
	 * <li>The filterDocument file contains a valid JSON document (syntax) but required JSON attributes are missing (semantic).</li>
	 * </ul>
	 */
	static public Filters setupFilters(String filterDocument) throws IOException {
		Path file = new File(filterDocument).toPath();
		try(InputStream inputStream = Files.newInputStream(file)) {
			return setupFilters(inputStream);
		}
	}

	/**
	 * Read the filterDocument input stream, parse its JSON-formatted content, and
	 * build a filter tree.
	 * 
	 * @param inputStream
	 * Specifies the input stream that provides a JSON-formatted text, which
	 * contains the specification of the filter criteria for domain, instance,
	 * job and operator.
	 * 
	 * @return
	 * A tree of filter objects that is used to evaluate whether a given
	 * domain, instance, job or operator name matches the specified
	 * filter criteria.
	 * 
	 * @throws IOException
	 * This exception is thrown under the following conditions.
	 * <ul>
	 * <li>An I/O error occurred, for example, the input stream fails to read data.</li>
	 * <li>The filterDocument contains an invalid JSON document.</li>
	 * <li>The filterDocument contains a valid JSON document (syntax) but required JSON attributes are missing (semantic).</li>
	 * </ul>
	 */
	static public Filters setupFilters(InputStream inputStream) throws IOException {
		Filters filters = new Filters();
		JSONArtifact root = JSON.parse(inputStream);
		DomainParser domainParser = new DomainParser();
		if (domainParser.validate(root)) {
			Set<DomainFilter> domainFilters = domainParser.buildFilters(root);
			for (DomainFilter domainFilter : domainFilters) {
				filters._domainFilters.put(domainFilter.getRegularExpression(), domainFilter);
			}
		}
		return filters;
	}

}
