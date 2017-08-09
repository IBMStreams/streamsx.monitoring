//
// ****************************************************************************
// * Copyright (C) 2016, 2017, International Business Machines Corporation    *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.jmx.internal.filters;

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
import com.ibm.streamsx.monitoring.jmx.OperatorConfiguration.OpType;

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

	public boolean matchesOperatorMetricName(String domainId, String instanceId, String jobName, String operatorName, String metricName) {
		boolean matches = false;
		for(DomainFilter filter : _domainFilters.values()) {
			matches = filter.matchesOperatorMetricName(domainId, instanceId, jobName, operatorName, metricName);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesOperatorInputPortIndex(String domainId, String instanceId, String jobName, String operatorName, Integer portIndex) {
		boolean matches = false;
		for(DomainFilter filter : _domainFilters.values()) {
			matches = filter.matchesOperatorInputPortIndex(domainId, instanceId, jobName, operatorName, portIndex);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesOperatorInputPortMetricName(String domainId, String instanceId, String jobName, String operatorName, Integer portIndex, String metricName) {
		boolean matches = false;
		for(DomainFilter filter : _domainFilters.values()) {
			matches = filter.matchesOperatorInputPortMetricName(domainId, instanceId, jobName, operatorName, portIndex, metricName);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesOperatorOutputPortIndex(String domainId, String instanceId, String jobName, String operatorName, Integer portIndex) {
		boolean matches = false;
		for(DomainFilter filter : _domainFilters.values()) {
			matches = filter.matchesOperatorOutputPortIndex(domainId, instanceId, jobName, operatorName, portIndex);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesOperatorOutputPortMetricName(String domainId, String instanceId, String jobName, String operatorName, Integer portIndex, String metricName) {
		boolean matches = false;
		for(DomainFilter filter : _domainFilters.values()) {
			matches = filter.matchesOperatorOutputPortMetricName(domainId, instanceId, jobName, operatorName, portIndex, metricName);
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

	public boolean matchesPeMetricName(String domainId, String instanceId, String jobName, BigInteger peId, String metricName) {
		boolean matches = false;
		for(DomainFilter filter : _domainFilters.values()) {
			matches = filter.matchesPeMetricName(domainId, instanceId, jobName, peId, metricName);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesPeInputPortIndex(String domainId, String instanceId, String jobName, BigInteger peId, Integer portIndex) {
		boolean matches = false;
		for(DomainFilter filter : _domainFilters.values()) {
			matches = filter.matchesPeInputPortIndex(domainId, instanceId, jobName, peId, portIndex);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesPeInputPortMetricName(String domainId, String instanceId, String jobName, BigInteger peId, Integer portIndex, String metricName) {
		boolean matches = false;
		for(DomainFilter filter : _domainFilters.values()) {
			matches = filter.matchesPeInputPortMetricName(domainId, instanceId, jobName, peId, portIndex, metricName);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesPeOutputPortIndex(String domainId, String instanceId, String jobName, BigInteger peId, Integer portIndex) {
		boolean matches = false;
		for(DomainFilter filter : _domainFilters.values()) {
			matches = filter.matchesPeOutputPortIndex(domainId, instanceId, jobName, peId, portIndex);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesPeOutputPortMetricName(String domainId, String instanceId, String jobName, BigInteger peId, Integer portIndex, String metricName) {
		boolean matches = false;
		for(DomainFilter filter : _domainFilters.values()) {
			matches = filter.matchesPeOutputPortMetricName(domainId, instanceId, jobName, peId, portIndex, metricName);
			if (matches) {
				break;
			}
		}
		return matches;
	}
	
	public boolean matchesPeConnectionId(String domainId, String instanceId, String jobName, BigInteger peId, String connectionId) {
		boolean matches = false;
		for(DomainFilter filter : _domainFilters.values()) {
			matches = filter.matchesPeConnectionId(domainId, instanceId, jobName, peId, connectionId);
			if (matches) {
				break;
			}
		}
		return matches;
	}
	
	public boolean matchesPeConnectionMetricName(String domainId, String instanceId, String jobName, BigInteger peId, String connectionId, String metricName) {
		boolean matches = false;
		for(DomainFilter filter : _domainFilters.values()) {
			matches = filter.matchesPeConnectionMetricName(domainId, instanceId, jobName, peId, connectionId, metricName);
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
	 * operator, and metric names.
	 * 
	 * @return
	 * A tree of filter objects that is used to evaluate whether a given
	 * domain, instance, job, operator, or metric name matches the specified
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
	static public Filters setupFilters(String filterDocument, OpType aType) throws IOException {
		Path file = new File(filterDocument).toPath();
		try(InputStream inputStream = Files.newInputStream(file)) {
			return setupFilters(inputStream, aType);
		}
	}

	/**
	 * Read the filterDocument input stream, parse its JSON-formatted content, and
	 * build a filter tree.
	 * 
	 * @param inputStream
	 * Specifies the input stream that provides a JSON-formatted text, which
	 * contains the specification of the filter criteria for domain, instance,
	 * job, operator, and metric names.
	 * 
	 * @return
	 * A tree of filter objects that is used to evaluate whether a given
	 * domain, instance, job, operator, or metric name matches the specified
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
	static public Filters setupFilters(InputStream inputStream, OpType aType) throws IOException {
		Filters filters = new Filters();
		JSONArtifact root = JSON.parse(inputStream);
		DomainParser domainParser = new DomainParser(aType);
		if (domainParser.validate(root)) {
			Set<DomainFilter> domainFilters = domainParser.buildFilters(root);
			for (DomainFilter domainFilter : domainFilters) {
				filters._domainFilters.put(domainFilter.getRegularExpression(), domainFilter);
			}
		}
		return filters;
	}

}
