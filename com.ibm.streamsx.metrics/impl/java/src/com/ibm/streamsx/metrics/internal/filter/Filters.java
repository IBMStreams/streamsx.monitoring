package com.ibm.streamsx.metrics.internal.filter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

import com.ibm.json.java.JSON;
import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONArtifact;
import com.ibm.json.java.JSONObject;

/**
 * 
 */
public class Filters extends Filter {

	public Filters() {
		super("---noname---");
	}

	public void add(String domainNameRegEx, String instanceNameRegEx, String jobNameRegEx, String operatorNameRegEx, String metricNameRegEx) {
		if (!_filters.containsKey(domainNameRegEx)) {
			addFilter(new DomainFilter(domainNameRegEx));
		}
		((DomainFilter)_filters.get(domainNameRegEx)).add(instanceNameRegEx, jobNameRegEx, operatorNameRegEx, metricNameRegEx);
	}

	public boolean matches(String domainName) {
		return matches(domainName, null, null, null, null, 1);
	}

	public boolean matches(String domainName, String instanceName) {
		return matches(domainName, instanceName, null, null, null, 2);
	}

	public boolean matches(String domainName, String instanceName, String jobName) {
		return matches(domainName, instanceName, jobName, null, null, 3);
	}

	public boolean matches(String domainName, String instanceName, String jobName, String operatorName) {
		return matches(domainName, instanceName, jobName, operatorName, null, 4);
	}

	public boolean matches(String domainName, String instanceName, String jobName, String operatorName, String metricName) {
		return matches(domainName, instanceName, jobName, operatorName, metricName, 5);
	}

	protected boolean matches(String domainName, String instanceName, String jobName, String operatorName, String metricName, int stopLevel) {
		boolean matches = false;
		for(Filter filter : _filters.values()) {
			matches = ((DomainFilter)filter).matches(domainName, instanceName, jobName, operatorName, metricName, stopLevel);
			if (matches) {
				break;
			}
		}
		return matches;
	}
	
	static protected void addRegularExpressions(String filterDocument, String jsonAttributeName, JSONObject json, ArrayList<String> list) throws IOException {
		Object object = json.get(jsonAttributeName);
		if (object != null) {
			if (object instanceof JSONArray) {
				for (Object item : (JSONArray)object) {
					if (item instanceof String) {
						list.add((String)item);
					}
					else {
						throw new IOException(filterDocument + ": " + jsonAttributeName + " must be of String[] type but is: " + object.toString());
					}
				}
			}
			else if (object instanceof String){
				list.add((String)object);
			}
			else {
				throw new IOException(filterDocument + ": " + jsonAttributeName + " must be either of String or JSONArray type but is: " + object.toString());
			}
		}
		else {
			throw new IOException(filterDocument + ": missing " + jsonAttributeName + " attribute for: " + json.toString());
		}
	}

	static protected void setupFiltersFromJSONObject(String filterDocument, Filters filters, JSONObject json) throws IOException {
		ArrayList<String> domains = new ArrayList<String>();
		ArrayList<String> instances = new ArrayList<String>();
		ArrayList<String> jobs = new ArrayList<String>();
		ArrayList<String> operators = new ArrayList<String>();
		ArrayList<String> metrics = new ArrayList<String>();
		// Add regular expressions for domains, instances, jobs, operators, metrics.
		addRegularExpressions(filterDocument, "domainNames", json, domains);
		addRegularExpressions(filterDocument, "instanceNames", json, instances);
		addRegularExpressions(filterDocument, "jobNames", json, jobs);
		addRegularExpressions(filterDocument, "operatorNames", json, operators);
		addRegularExpressions(filterDocument, "metricNames", json, metrics);
		// Build the filter adding only those items for which all parts are defined.
		for(String domainNameRegEx : domains) {
			for(String instanceNameRegEx : instances) {
				for(String jobNameRegEx : jobs) {
					for(String operatorNameRegEx : operators) {
						for(String metricNameRegEx : metrics) {
//							System.out.format("add(domain=%s, instance=%s, job=%s, operator=%s, metric=%s)\n", domainNameRegEx, instanceNameRegEx, jobNameRegEx, operatorNameRegEx, metricNameRegEx);
							filters.add(domainNameRegEx, instanceNameRegEx, jobNameRegEx, operatorNameRegEx, metricNameRegEx);
						}
					}
				}
			}
		}
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
	static public Filters setupFilters(String filterDocument) throws IOException {
		Filters filters = new Filters();
		Path file = new File(filterDocument).toPath();
		InputStream inputStream = Files.newInputStream(file);
		try {
			JSONArtifact root = JSON.parse(inputStream);
			if (root instanceof JSONArray) {
				for (Object obj : (JSONArray)root) {
					if (obj instanceof JSONObject) {
						setupFiltersFromJSONObject(filterDocument, filters, (JSONObject)obj);
					}
				}
			}
			else if (root instanceof JSONObject) {
				setupFiltersFromJSONObject(filterDocument, filters, (JSONObject)root);
			}
		}
		finally {
			inputStream.close();
		}
		return filters;
	}

}
