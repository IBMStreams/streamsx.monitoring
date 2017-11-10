//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.jmx;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;

import com.ibm.streamsx.monitoring.jmx.internal.ConnectionNotificationTupleContainer;
import com.ibm.streamsx.monitoring.jmx.internal.DeltaMetricEvaluator;
import com.ibm.streamsx.monitoring.jmx.internal.EmitMetricTupleMode;
import com.ibm.streamsx.monitoring.jmx.internal.IMetricEvaluator;
import com.ibm.streamsx.monitoring.jmx.internal.PeriodicMetricEvaluator;
import com.ibm.streamsx.monitoring.jmx.internal.filters.Filters;
import com.ibm.streamsx.monitoring.jmx.internal.MetricsTupleContainer;
import com.ibm.streamsx.monitoring.jmx.internal.JobStatusTupleContainer;
import com.ibm.streamsx.monitoring.jmx.internal.LogTupleContainer;

/**
 * 
 */
public class OperatorConfiguration {

	/**
	 * Specifies the connection URL as returned by the {@code streamtool
	 * getjmxconnect} command.
	 */
	private String _connectionURL = null;

	/**
	 * Specifies the user that is required for the JMX connection.
	 */
	private String _user = null;

	/**
	 * Specifies the password that is required for the JMX connection.
	 */
	private String _password = null;

	/**
	 * Specifies the sslOption that is required for the JMX connection.
	 */
	private String _sslOption = null;	
	
	/**
	 * Specifies the domain that is monitored.
	 */
	private String _domainId = null;
	
	/**
	 * Specifies the IAM token endpoint.
	 */
	private String _iamTokenEndpoint = "https://iam.bluemix.net/oidc/token";

	/**
	 * Specifies the IAM API key.
	 */	
	private String _iamApiKey = null;

	/**
	 * Specifies the name of the application configuration object.
	 */
	private String _applicationConfigurationName = null;

	/**
	 * Specifies the path to a JSON-formatted document that specifies the
	 * domain, instance, job, operator, and metric name filters as regular
	 * expressions. Each regular expression must follow the rules that are
	 * specified for Java <a href="https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html">Pattern</a>.
	 */
	private String _filterDocument = null;

	/**
	 * Specifies the period after which a new metrics scan is initiated. The
	 * default is 5.0 seconds.
	 */
	private Double _scanPeriod = Double.valueOf(5.0);

	private JMXConnector _jmxConnector = null;
	
	private MBeanServerConnection _mbeanServerConnection = null;

	private Filters _filters = new Filters();

	private String _defaultFilterInstance = null;
	
	private String _defaultFilterDomain = null;	
	
	private MetricsTupleContainer _tupleContainerMetricsSource = null;
	
	private JobStatusTupleContainer _tupleContainerJobStatusSource = null;
	
	private LogTupleContainer _tupleContainerLogSource = null;
	
	private ConnectionNotificationTupleContainer _tupleContainerConnectionNotification = null;

	private EmitMetricTupleMode _emitMetricTuple = EmitMetricTupleMode.onChangedValue;
	
	private OpType _opType = OpType.METRICS_SOURCE;

	public enum OpType {
		METRICS_SOURCE,
		JOB_STATUS_SOURCE,
		LOG_SOURCE
	}
	public OpType get_OperatorType() {
		return _opType;
	}

	public void set_OperatorType(OpType type) {
		_opType = type;
	}
	
	public String get_connectionURL() {
		return _connectionURL;
	}

	public void set_connectionURL(String connectionURL) {
		if (!("".equals(connectionURL))) {
			this._connectionURL = connectionURL;
		}
	}

	public String get_user() {
		return _user;
	}

	public void set_user(String user) {
		if (!("".equals(user))) {
			this._user = user;
		}
	}

	public String get_password() {
		return _password;
	}

	public void set_password(String password) {
		if (!("".equals(password))) {
			this._password = password;
		}
	}

	public String get_iamTokenEndpoint() {
		return _iamTokenEndpoint;
	}

	public void set_iamTokenEndpoint(String iamTokenEndpoint) {
		if (!("".equals(iamTokenEndpoint))) {
			this._iamTokenEndpoint = iamTokenEndpoint;
		}
	}

	public String get_iamApiKey() {
		return _iamApiKey;
	}

	public void set_iamApiKey(String iamApiKey) {
		if (!("".equals(iamApiKey))) {
			this._iamApiKey = iamApiKey;
		}
	}

	public String get_sslOption() {
		return _sslOption;
	}

	public void set_sslOption(String sslOption) {
		if (!("".equals(sslOption))) {
			this._sslOption = sslOption;
		}
	}	

	public String get_domainId() {
		return _domainId;
	}

	public void set_domainId(String domainId) {
		if (!("".equals(domainId))) {
			this._domainId = domainId;
		}
	}

	public String get_applicationConfigurationName() {
		return _applicationConfigurationName;
	}

	public void set_applicationConfigurationName(String applicationConfigurationName) {
		if (!("".equals(applicationConfigurationName))) {
			this._applicationConfigurationName = applicationConfigurationName;
		}
	}

	public String get_filterDocument() {
		return _filterDocument;
	}

	public void set_filterDocument(String filterDocument) {
		if (!("".equals(filterDocument))) {
			this._filterDocument = filterDocument;
		}
	}

	public Double get_scanPeriod() {
		return _scanPeriod;
	}

	public void set_scanPeriod(Double scanPeriod) {
		this._scanPeriod = scanPeriod;
	}

	public JMXConnector get_jmxConnector() {
		return _jmxConnector;
	}

	public void set_jmxConnector(JMXConnector jmxConnector) {
		this._jmxConnector = jmxConnector;
	}

	public MBeanServerConnection get_mbeanServerConnection() {
		return _mbeanServerConnection;
	}

	public void set_mbeanServerConnection(MBeanServerConnection mbeanServerConnection) {
		this._mbeanServerConnection = mbeanServerConnection;
	}

	public Filters get_filters() {
		return _filters;
	}

	public void set_filters(Filters filters) {
		_filters = filters;
	}
	
	public MetricsTupleContainer get_tupleContainerMetricsSource() {
		return _tupleContainerMetricsSource;
	}

	public void set_tupleContainerMetricsSource(MetricsTupleContainer tupleContainer) {
		_tupleContainerMetricsSource = tupleContainer;
	}
	
	public LogTupleContainer get_tupleContainerLogSource() {
		return _tupleContainerLogSource;
	}

	public void set_tupleContainerLogSource(LogTupleContainer tupleContainer) {
		_tupleContainerLogSource = tupleContainer;
	}
	
	public JobStatusTupleContainer get_tupleContainerJobStatusSource() {
		return _tupleContainerJobStatusSource;
	}

	public void set_tupleContainerJobStatusSource(JobStatusTupleContainer tupleContainer) {
		_tupleContainerJobStatusSource = tupleContainer;
	}	

	public void set_tupleContainerConnectionNotification(ConnectionNotificationTupleContainer tupleContainer) {
		_tupleContainerConnectionNotification = tupleContainer;
	}
	
	public ConnectionNotificationTupleContainer get_tupleContainerConnectionNotification() {
		return _tupleContainerConnectionNotification;
	}
	
	public void set_emitMetricTuple(EmitMetricTupleMode mode) {
		_emitMetricTuple = mode;
	}

	public IMetricEvaluator newDefaultMetricEvaluator() {
		switch(_emitMetricTuple) {
		case onChangedValue:
			return new DeltaMetricEvaluator();
		case periodic:
			return PeriodicMetricEvaluator.getSingleton();
		default:
			return null;
		}
	}
	
	public void set_defaultFilterInstance(String instance) {
		_defaultFilterInstance = instance;
	}

	public void set_defaultFilterDomain(String domain) {
		_defaultFilterDomain = domain;
	}
	
	public String get_defaultFilterDocument() {
		String result = null;
		if (_opType.equals(OpType.METRICS_SOURCE)) {
			result = "[{\"domainIdPatterns\":\""
					+ _defaultFilterDomain
					+ "\",\"instances\":[{\"instanceIdPatterns\":\""
					+ _defaultFilterInstance
					+ "\",\"jobs\":[{\"jobNamePatterns\":\".*\","
					+ "\"pes\":[{\"metricNamePatterns\":\".*\",\"inputPorts\":[{\"portIndexes\":\"*\",\"metricNamePatterns\":\".*\"}],\"outputPorts\":[{\"portIndexes\":\"*\",\"metricNamePatterns\":\".*\"}],\"connections\":[{\"connectionIdPatterns\":\".*\",\"metricNamePatterns\":\".*\"}]}],"
					+ "\"operators\":[{\"operatorNamePatterns\":\".*\",\"metricNamePatterns\":\".*\",\"inputPorts\":[{\"portIndexes\":\"*\",\"metricNamePatterns\":\".*\"}],\"outputPorts\":[{\"portIndexes\":\"*\",\"metricNamePatterns\":\".*\"}]}]}]}]}]";
		}
		else if (_opType.equals(OpType.JOB_STATUS_SOURCE)) {
			result = "[{\"domainIdPatterns\":\""
					+ _defaultFilterDomain
					+ "\",\"instances\":[{\"instanceIdPatterns\":\""
					+ _defaultFilterInstance
					+ "\",\"jobs\":[{\"jobNamePatterns\":\".*\",}]}]}]";	
		}
		return result;
	}

}
