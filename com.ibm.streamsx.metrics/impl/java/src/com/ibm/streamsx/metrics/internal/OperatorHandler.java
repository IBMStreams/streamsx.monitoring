package com.ibm.streamsx.metrics.internal;

import org.apache.log4j.Logger;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.management.JMX;
import javax.management.ObjectName;

import com.ibm.streams.management.Metric;
import com.ibm.streams.management.ObjectNameBuilder;
import com.ibm.streams.management.job.OperatorMXBean;

/**
 * 
 */
public class OperatorHandler {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(OperatorHandler.class.getName());

	private OperatorConfiguration _operatorConfiguration = null;

	private String _domainName = null;

	private String _instanceName = null;
	
	private BigInteger _jobId = null;
	
	private String _jobName = null;
	
	private String _operatorName = null;
	
	private OperatorMXBean _operator = null;

	private Map<String /* metric name */, Metric> _capturedCustomMetrics = new HashMap<String, Metric>();
	
	private Set<String /* metric name */> _uncapturedCustomMetrics = new HashSet<String>();
	
	private Map<Integer /* port index */, Map<String /* metric Name*/, Metric>> _capturedInputPortMetrics = new HashMap<Integer, Map<String, Metric>>();

	private Map<Integer /* port index */, Map<String /* metric Name*/, Metric>> _capturedOutputPortMetrics = new HashMap<Integer, Map<String, Metric>>();

	public OperatorHandler(OperatorConfiguration operatorConfiguration, String domainName, String instanceName, BigInteger jobId, String jobName, String operatorName) {

		if (_trace.isDebugEnabled()) {
			_trace.debug("OperatorHandler(" + domainName + "," + instanceName + ")");
		}
		// Store parameters for later use.
		_operatorConfiguration = operatorConfiguration;
		_domainName = domainName;
		_instanceName = instanceName;
		_jobId = jobId;
		_jobName = jobName;
		_operatorName = operatorName;

		ObjectName operatorObjName = ObjectNameBuilder.operator(_domainName, _instanceName, _jobId, _operatorName);
		_operator = JMX.newMXBeanProxy(_operatorConfiguration.get_mbeanServerConnection(), operatorObjName, OperatorMXBean.class, true);
		
		/*
		 * Register input port metrics that match the specified filter criteria.
		 */
		for (Integer portIndex : _operator.getInputPorts()) {
			
		}
		/*
		 * Register output port metrics that match the specified filter criteria.
		 */
		for (Integer portIndex : _operator.getOutputPorts()) {
			
		}
	}

	/**
	 * Iterate all jobs to capture the job metrics.
	 * @throws Exception 
	 */
	public void captureMetrics() throws Exception {
		if (_trace.isDebugEnabled()) {
			_trace.debug("--> captureMetrics(domain=" + _domainName + ",instance=" + _instanceName + ")");
		}
		_operatorConfiguration.get_tupleContainer().setOperatorName(_operatorName);
		/*
		 * The custom metrics can be created in an operator's state, onTuple,
		 * or onPunct clause. Therefore, it is not ensured that the custom
		 * metric is available as soon as the operator is available.
		 * 
		 * The OperatorContextMXBean supports a CUSTOM_METRIC_CREATED
		 * notification, but this notification is not available via the
		 * JMX API. It seems as if it is available only within an operator,
		 * for this specific operator.
		 * 
		 * The implemented solution is not optimal:
		 * 
		 * Always retrieve the custom metrics. If a custom metric was not
		 * handled before, verify whether its name matches the filters.
		 * If it matches the filters, store the metric in the
		 * _capturedCustomMetrics map. If it does not match, store the name
		 * in the _uncapturedCustomMetrics set. If a custom metric was handled
		 * before, check whether it is either in the _capturedCustomMetrics map
		 * or the _uncapturedCustomMetrics set, and act accordingly.
		 */
		for (Metric metric : _operator.retrieveMetrics(false)) {
			String metricName = metric.getName();
			/*
			 * Metric shall be captured.
			 */
			if (_capturedCustomMetrics.containsKey(metricName)) {
				Metric lastCapturedState = _capturedCustomMetrics.get(metricName);
				if (lastCapturedState.getValue() != metric.getValue()) {
					_capturedCustomMetrics.put(metricName, metric);
					submitMetric(metric);
				}
			}
			/*
			 * Metric shall be ignored.
			 */
			else if (_uncapturedCustomMetrics.contains(metricName)) {
				// Ignore this metric because it does not match the filters.
			}
			/*
			 * Decide whether the metric shall be captured or ignored.
			 */
			else if(_operatorConfiguration.get_filters().matches(_domainName, _instanceName, _jobName, _operatorName, metricName)) {
				_trace.error("The following custom metric meets the filter criteria and is therefore, monitored: domain=" + _domainName + ", instance=" + _instanceName + ", job=[" + _jobId + "][" + _jobName + "], operator=" + _operatorName + ", metric[custom]=" + metricName);
				_capturedCustomMetrics.put(metricName, metric);
				submitMetric(metric);
			}
			else { // TODO if (_trace.isInfoEnabled()) {
				_trace.error("The following custom metric does not meet the filter criteria and is therefore, not monitored: domain=" + _domainName + ", instance=" + _instanceName + ", job=[" + _jobId + "][" + _jobName + "], operator=" + _operatorName + ", metric[custom]=" + metricName);
				_uncapturedCustomMetrics.add(metricName);
			}
		}
		if (_trace.isDebugEnabled()) {
			_trace.debug("<-- captureMetrics(domain=" + _domainName + ",instance=" + _instanceName + ")");
		}
	}

	protected void submitMetric(Metric metric) throws Exception {
		_operatorConfiguration.get_tupleContainer().setMetricName(metric.getName());
		_operatorConfiguration.get_tupleContainer().setMetricValue(metric.getValue());
		_operatorConfiguration.get_tupleContainer().setLastTimeRetrieved(metric.getLastTimeRetrieved());
		_operatorConfiguration.get_tupleContainer().submit();
	}
	
}
