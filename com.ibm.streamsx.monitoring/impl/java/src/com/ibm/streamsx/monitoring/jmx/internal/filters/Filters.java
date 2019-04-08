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
	protected Map<String /* regular expression */, InstanceFilter> _instanceFilters = new HashMap<>();

	public Filters() {
	}

	public boolean matchesInstanceId(String instanceId) {
		boolean matches = false;
		for(InstanceFilter filter : _instanceFilters.values()) {
			matches = filter.matchesInstanceId(instanceId);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesJobName(String instanceId, String jobName) {
		boolean matches = false;
		for(InstanceFilter filter : _instanceFilters.values()) {
			matches = filter.matchesJobName(instanceId, jobName);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesOperatorName(String instanceId, String jobName, String operatorName) {
		boolean matches = false;
		for(InstanceFilter filter : _instanceFilters.values()) {
			matches = filter.matchesOperatorName(instanceId, jobName, operatorName);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesOperatorMetricName(String instanceId, String jobName, String operatorName, String metricName) {
		boolean matches = false;
		for(InstanceFilter filter : _instanceFilters.values()) {
			matches = filter.matchesOperatorMetricName(instanceId, jobName, operatorName, metricName);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesOperatorInputPortIndex(String instanceId, String jobName, String operatorName, Integer portIndex) {
		boolean matches = false;
		for(InstanceFilter filter : _instanceFilters.values()) {
			matches = filter.matchesOperatorInputPortIndex(instanceId, jobName, operatorName, portIndex);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesOperatorInputPortMetricName(String instanceId, String jobName, String operatorName, Integer portIndex, String metricName) {
		boolean matches = false;
		for(InstanceFilter filter : _instanceFilters.values()) {
			matches = filter.matchesOperatorInputPortMetricName(instanceId, jobName, operatorName, portIndex, metricName);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesOperatorOutputPortIndex(String instanceId, String jobName, String operatorName, Integer portIndex) {
		boolean matches = false;
		for(InstanceFilter filter : _instanceFilters.values()) {
			matches = filter.matchesOperatorOutputPortIndex(instanceId, jobName, operatorName, portIndex);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesOperatorOutputPortMetricName(String instanceId, String jobName, String operatorName, Integer portIndex, String metricName) {
		boolean matches = false;
		for(InstanceFilter filter : _instanceFilters.values()) {
			matches = filter.matchesOperatorOutputPortMetricName(instanceId, jobName, operatorName, portIndex, metricName);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesPeId(String instanceId, String jobName, BigInteger peId) {
		boolean matches = false;
		for(InstanceFilter filter : _instanceFilters.values()) {
			matches = filter.matchesPeId(instanceId, jobName, peId);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesPeMetricName(String instanceId, String jobName, BigInteger peId, String metricName) {
		boolean matches = false;
		for(InstanceFilter filter : _instanceFilters.values()) {
			matches = filter.matchesPeMetricName(instanceId, jobName, peId, metricName);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesPeInputPortIndex(String instanceId, String jobName, BigInteger peId, Integer portIndex) {
		boolean matches = false;
		for(InstanceFilter filter : _instanceFilters.values()) {
			matches = filter.matchesPeInputPortIndex(instanceId, jobName, peId, portIndex);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesPeInputPortMetricName(String instanceId, String jobName, BigInteger peId, Integer portIndex, String metricName) {
		boolean matches = false;
		for(InstanceFilter filter : _instanceFilters.values()) {
			matches = filter.matchesPeInputPortMetricName(instanceId, jobName, peId, portIndex, metricName);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesPeOutputPortIndex(String instanceId, String jobName, BigInteger peId, Integer portIndex) {
		boolean matches = false;
		for(InstanceFilter filter : _instanceFilters.values()) {
			matches = filter.matchesPeOutputPortIndex(instanceId, jobName, peId, portIndex);
			if (matches) {
				break;
			}
		}
		return matches;
	}

	public boolean matchesPeOutputPortMetricName(String instanceId, String jobName, BigInteger peId, Integer portIndex, String metricName) {
		boolean matches = false;
		for(InstanceFilter filter : _instanceFilters.values()) {
			matches = filter.matchesPeOutputPortMetricName(instanceId, jobName, peId, portIndex, metricName);
			if (matches) {
				break;
			}
		}
		return matches;
	}
	
	public boolean matchesPeConnectionId(String instanceId, String jobName, BigInteger peId, String connectionId) {
		boolean matches = false;
		for(InstanceFilter filter : _instanceFilters.values()) {
			matches = filter.matchesPeConnectionId(instanceId, jobName, peId, connectionId);
			if (matches) {
				break;
			}
		}
		return matches;
	}
	
	public boolean matchesPeConnectionMetricName(String instanceId, String jobName, BigInteger peId, String connectionId, String metricName) {
		boolean matches = false;
		for(InstanceFilter filter : _instanceFilters.values()) {
			matches = filter.matchesPeConnectionMetricName(instanceId, jobName, peId, connectionId, metricName);
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
	 * the specification of the filter criteria for instance, job,
	 * operator, and metric names.
	 * 
	 * @return
	 * A tree of filter objects that is used to evaluate whether a given
	 * instance, job, operator, or metric name matches the specified
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
	 * contains the specification of the filter criteria for instance,
	 * job, operator, and metric names.
	 * 
	 * @return
	 * A tree of filter objects that is used to evaluate whether a given
	 * instance, job, operator, or metric name matches the specified
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
		InstanceParser instanceParser = new InstanceParser(aType);
		if (instanceParser.validate(root)) {
			Set<InstanceFilter> instanceFilters = instanceParser.buildFilters(root);
			for (InstanceFilter instanceFilter : instanceFilters) {
				filters._instanceFilters.put(instanceFilter.getRegularExpression(), instanceFilter);
			}
		}
		return filters;
	}

}
