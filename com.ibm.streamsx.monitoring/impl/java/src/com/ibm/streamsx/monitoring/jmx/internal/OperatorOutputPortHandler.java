//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.jmx.internal;

import org.apache.log4j.Logger;

import java.math.BigInteger;
import java.util.Set;

import javax.management.JMX;
import javax.management.ObjectName;

import com.ibm.streams.management.Metric;
import com.ibm.streams.management.ObjectNameBuilder;
import com.ibm.streams.management.job.OperatorOutputPortMXBean;
import com.ibm.streamsx.monitoring.jmx.OperatorConfiguration;

/**
 * 
 */
public class OperatorOutputPortHandler extends MetricOwningHandler implements Closeable {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(OperatorOutputPortHandler.class.getName());

	private String _instanceId = null;
	
	private String _jobId = null;
	
	private String _jobName = null;
	
	private String _operatorName = null;
	
	private Integer _portIndex = null;
	
	private OperatorOutputPortMXBean _port = null;

	public OperatorOutputPortHandler(OperatorConfiguration operatorConfiguration, String instanceId, String jobId, String jobName, String operatorName, Integer portIndex) {

		super(MetricsRegistrationMode.DynamicMetricsRegistration);

		// Determine the trace level status once per function.
		boolean isDebugEnabled = _trace.isDebugEnabled();

		// Store parameters for later use.
		_operatorConfiguration = operatorConfiguration;
		_instanceId = instanceId;
		_jobId = jobId;
		_jobName = jobName;
		_operatorName = operatorName;
		_portIndex = portIndex;

		if (isDebugEnabled) {
			_trace.debug("--> OutputPortHandler(instance=" + _instanceId + ", job=[" + _jobId + "]:" + _jobName + ", operator=" + _operatorName + ", port=" + _portIndex + ")");
		}
		
		ObjectName operatorObjName = ObjectNameBuilder.operatorOutputPort(_instanceId, _jobId, _operatorName, _portIndex);
		_port = JMX.newMXBeanProxy(_operatorConfiguration.get_mbeanServerConnection(), operatorObjName, OperatorOutputPortMXBean.class, true);
		
		/*
		 * Register output port metrics that match the specified filter criteria.
		 */
		registerMetrics();
		
		if (isDebugEnabled) {
			_trace.debug("<-- OutputPortHandler(instance=" + _instanceId + ", job=[" + _jobId + "]:" + _jobName + ", operator=" + _operatorName + ", port=" + _portIndex + ")");
		}
	}

	@Override
	protected boolean isRelevantMetric(String metricName) {
		boolean isRelevant = _operatorConfiguration.get_filters().matchesOperatorOutputPortMetricName(_instanceId, _jobName, _operatorName, _portIndex, metricName);
		if (_trace.isInfoEnabled()) {
			if (isRelevant) {
				_trace.info("The following output port metric meets the filter criteria and is therefore, monitored: instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], operator=" + _operatorName + ", port=" + _portIndex + ", metric=" + metricName);
			}
			else { 
				_trace.info("The following output port metric does not meet the filter criteria and is therefore, not monitored: instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], operator=" + _operatorName + ", port=" + _portIndex + ", metric=" + metricName);
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
			_trace.debug("--> captureMetrics(instance=" + _instanceId + ", job=[" + _jobId + "]:" + _jobName + ", operator=" + _operatorName + ", port=" + _portIndex + ")");
		}
		MetricsTupleContainer tc = _operatorConfiguration.get_tupleContainerMetricsSource();
		tc.setOrigin("OperatorOutputPort");
		tc.setPortIndex(_portIndex);
		captureAndSubmitChangedMetrics();

		if (isDebugEnabled) {
			_trace.debug("<-- captureMetrics(instance=" + _instanceId + ", job=[" + _jobId + "]:" + _jobName + ", operator=" + _operatorName + ", port=" + _portIndex + ")");
		}
	}

}
