package com.ibm.streamsx.metrics.internal;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;

import com.ibm.streamsx.metrics.internal.filter.Filters;

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
	 * Specifies the domain that is monitored.
	 */
	private String _domainId = null;

	/**
	 * Specifies the period after which a failed JMX connect is repeated. The
	 * default is 10.0 seconds.
	 */
	private Double _retryPeriod = Double.valueOf(10.0);

	/**
	 * Specifies the retry count for failed JMX connects. The default is -1,
	 * which means infinite retries.
	 */
	private Integer _retryCount = Integer.valueOf(-1);

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

	private TupleContainer _tupleContainer = null;

	private EmitMetricTupleMode _emitMetricTuple = EmitMetricTupleMode.onChangedValue;
	
	public String get_connectionURL() {
		return _connectionURL;
	}

	public void set_connectionURL(String connectionURL) {
		this._connectionURL = connectionURL;
	}

	public String get_user() {
		return _user;
	}

	public void set_user(String user) {
		this._user = user;
	}

	public String get_password() {
		return _password;
	}

	public void set_password(String password) {
		this._password = password;
	}

	public String get_domainId() {
		return _domainId;
	}

	public void set_domainId(String domainId) {
		this._domainId = domainId;
	}

	public Double get_retryPeriod() {
		return _retryPeriod;
	}

	public void set_retryPeriod(Double retryPeriod) {
		this._retryPeriod = retryPeriod;
	}

	public Integer get_retryCount() {
		return _retryCount;
	}

	public void set_retryCount(Integer retryCount) {
		this._retryCount = retryCount;
	}

	public String get_filterDocument() {
		return _filterDocument;
	}

	public void set_filterDocument(String filterDocument) {
		this._filterDocument = filterDocument;
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
	
	public TupleContainer get_tupleContainer() {
		return _tupleContainer ;
	}

	public void set_tupleContainer(TupleContainer tupleContainer) {
		_tupleContainer = tupleContainer;
		
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

}
