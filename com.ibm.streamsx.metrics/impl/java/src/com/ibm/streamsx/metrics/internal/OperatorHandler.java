package com.ibm.streamsx.metrics.internal;

import org.apache.log4j.Logger;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
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
	
	private Map<Integer /* port index */, Map<String /* metric Name*/, Metric>> _capturedInputPortMetrics = new HashMap<Integer, Map<String, Metric>>();

	private Map<Integer /* port index */, Map<String /* metric Name*/, Metric>> _capturedOutputPortMetrics = new HashMap<Integer, Map<String, Metric>>();

	private BigInteger _lastTimeRetrieved = null;
	
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
		 * Register custom metrics that match the specified filter criteria.
		 */
		for (Metric metric : _operator.retrieveMetrics(false)) {
			if(_operatorConfiguration.get_filters().matches(_domainName, _instanceName, _jobName, _operatorName, metric.getName())) {
				_trace.error("The following custom metric meets the filter criteria and is therefore, monitored: domain=" + _domainName + ", instance=" + _instanceName + ", job=[" + _jobId + "][" + _jobName + "], operator=" + operatorName + ", metric[custom]=" + metric.getName());
				_capturedCustomMetrics.put(metric.getName(), null);
			}
			else { // TODO if (_trace.isInfoEnabled()) {
				_trace.error("The following custom metric does not meet the filter criteria and is therefore, not monitored: domain=" + _domainName + ", instance=" + _instanceName + ", job=[" + _jobId + "][" + _jobName + "], operator=" + operatorName + ", metric[custom]=" + metric.getName());
			}
		}
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
		// Evaluate custom metrics.
		if (_capturedCustomMetrics.size() > 0) {
			for (Metric metric : _operator.retrieveMetrics(false)) {
				Metric lastCapturedState = _capturedCustomMetrics.get(metric.getName());
				if (lastCapturedState != null) {
					if (lastCapturedState.getValue() != metric.getValue()) {
						_capturedCustomMetrics.put(metric.getName(), null);
						submitMetric(metric);
					}
				}
				else {
					_capturedCustomMetrics.put(metric.getName(), null);
					submitMetric(metric);
				}
			}
		}
		if (_trace.isDebugEnabled()) {
			_trace.debug("<-- captureMetrics(domain=" + _domainName + ",instance=" + _instanceName + ")");
		}
	}

	protected void submitMetric(Metric metric) throws Exception {
		_operatorConfiguration.get_tupleContainer().setMetricName(metric.getName());
		_operatorConfiguration.get_tupleContainer().setMetricValue(metric.getValue());
		_operatorConfiguration.get_tupleContainer().submit();
	}
	
}
