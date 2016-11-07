package com.ibm.streamsx.metrics.internal;

import org.apache.log4j.Logger;

import java.math.BigInteger;
import java.util.Set;

import javax.management.JMX;
import javax.management.ObjectName;

import com.ibm.streams.management.Metric;
import com.ibm.streams.management.ObjectNameBuilder;
import com.ibm.streams.management.job.OperatorOutputPortMXBean;

/**
 * 
 */
public class OutputPortHandler extends MetricOwningHandler {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(OutputPortHandler.class.getName());

	private String _domainName = null;

	private String _instanceName = null;
	
	private BigInteger _jobId = null;
	
	private String _jobName = null;
	
	private String _operatorName = null;
	
	private Integer _portIndex = null;
	
	private OperatorOutputPortMXBean _port = null;

	public OutputPortHandler(OperatorConfiguration operatorConfiguration, String domainName, String instanceName, BigInteger jobId, String jobName, String operatorName, Integer portIndex) {

		super(MetricsRegistrationMode.DynamicMetricsRegistration);

		// Determine the trace level status once per function.
		boolean isDebugEnabled = _trace.isDebugEnabled();

		// Store parameters for later use.
		_operatorConfiguration = operatorConfiguration;
		_domainName = domainName;
		_instanceName = instanceName;
		_jobId = jobId;
		_jobName = jobName;
		_operatorName = operatorName;
		_portIndex = portIndex;

		if (isDebugEnabled) {
			_trace.debug("--> OutputPortHandler(domain=" + _domainName + ", instance=" + _instanceName + ", job=[" + _jobId + "]:" + _jobName + ", operator=" + _operatorName + ", port=" + _portIndex + ")");
		}
		
		ObjectName operatorObjName = ObjectNameBuilder.operatorOutputPort(_domainName, _instanceName, _jobId, _operatorName, _portIndex);
		_port = JMX.newMXBeanProxy(_operatorConfiguration.get_mbeanServerConnection(), operatorObjName, OperatorOutputPortMXBean.class, true);
		
		/*
		 * Register output port metrics that match the specified filter criteria.
		 */
		registerMetrics();
		
		if (isDebugEnabled) {
			_trace.debug("<-- OutputPortHandler(domain=" + _domainName + ", instance=" + _instanceName + ", job=[" + _jobId + "]:" + _jobName + ", operator=" + _operatorName + ", port=" + _portIndex + ")");
		}
	}

	@Override
	protected boolean isRelevantMetric(String metricName) {
		// TODO Enhance with port filter.
		boolean isRelevant = _operatorConfiguration.get_filters().matches(_domainName, _instanceName, _jobName, _operatorName, metricName);
		if (true || _trace.isInfoEnabled()) { // TODO remove "true ||"
			if (isRelevant) {
				_trace.error("The following output port metric meets the filter criteria and is therefore, monitored: domain=" + _domainName + ", instance=" + _instanceName + ", job=[" + _jobId + "][" + _jobName + "], operator=" + _operatorName + ", port=" + _portIndex + ", metric=" + metricName);
			}
			else { 
				_trace.error("The following output port metric does not meet the filter criteria and is therefore, not monitored: domain=" + _domainName + ", instance=" + _instanceName + ", job=[" + _jobId + "][" + _jobName + "], operator=" + _operatorName + ", port=" + _portIndex + ", metric=" + metricName);
			}
		}
		return isRelevant;
	}

	@Override
	protected Set<Metric> retrieveMetrics() {
		Set<Metric> metrics = _port.retrieveMetrics();
		return metrics;
	}

	/**
	 * Iterate all jobs to capture the job metrics.
	 * @throws Exception 
	 */
	public void captureMetrics() throws Exception {

		// Determine the trace level status once per function.
		boolean isDebugEnabled = _trace.isDebugEnabled();

		if (isDebugEnabled) {
			_trace.debug("--> captureMetrics(domain=" + _domainName + ", instance=" + _instanceName + ", job=[" + _jobId + "]:" + _jobName + ", operator=" + _operatorName + ", port=" + _portIndex + ")");
		}

		_operatorConfiguration.get_tupleContainer().setOrigin("OperatorOutputPort");
		_operatorConfiguration.get_tupleContainer().setPortIndex(_portIndex);
		captureAndSubmitChangedMetrics();

		if (isDebugEnabled) {
			_trace.debug("<-- captureMetrics(domain=" + _domainName + ", instance=" + _instanceName + ", job=[" + _jobId + "]:" + _jobName + ", operator=" + _operatorName + ", port=" + _portIndex + ")");
		}
	}

}
