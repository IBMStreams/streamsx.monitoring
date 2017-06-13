//
//****************************************************************************
//* Copyright (C) 2017, International Business Machines Corporation          *
//* All rights reserved.                                                     *
//****************************************************************************
//

package com.ibm.streamsx.metrics.internal;

import org.apache.log4j.Logger;

import java.math.BigInteger;
import java.util.Set;

import javax.management.JMX;
import javax.management.ObjectName;

import com.ibm.streams.management.Metric;
import com.ibm.streams.management.ObjectNameBuilder;
import com.ibm.streams.management.job.PeConnectionMXBean;

/**
* 
*/
public class PeConnectionHandler extends MetricOwningHandler implements Closeable {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(PeConnectionHandler.class.getName());

	private String _domainId = null;

	private String _instanceId = null;
	
	private BigInteger _jobId = null;
	
	private String _jobName = null;
	
	private BigInteger _peId = null;
	
	private String _connectionId = null;
	
	private PeConnectionMXBean _connection = null;

	public PeConnectionHandler(OperatorConfiguration operatorConfiguration, String domainId, String instanceId, BigInteger jobId, String jobName, BigInteger peId, String connectionId) {

		super(MetricsRegistrationMode.DynamicMetricsRegistration);

		// Determine the trace level status once per function.
		boolean isDebugEnabled = _trace.isDebugEnabled();

		// Store parameters for later use.
		_operatorConfiguration = operatorConfiguration;
		_domainId = domainId;
		_instanceId = instanceId;
		_jobId = jobId;
		_jobName = jobName;
		_peId = peId;
		_connectionId = connectionId;

		if (isDebugEnabled) {
			_trace.debug("--> ConnectionHandler(domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "]:" + _jobName + ", peId=" + _peId + ", connectionId=" + _connectionId + ")");
		}
		
		ObjectName objName = ObjectNameBuilder.peConnection(_domainId, _instanceId, _connectionId);
		_connection = JMX.newMXBeanProxy(_operatorConfiguration.get_mbeanServerConnection(), objName, PeConnectionMXBean.class, true);
		
		/*
		 * Register connection metrics that match the specified filter criteria.
		 */
		registerMetrics();
		
		if (isDebugEnabled) {
			_trace.debug("<-- PeConnectionHandler(domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "]:" + _jobName + ", peId=" + _peId + ", connectionId=" + _connectionId + ")");
		}
	}

	@Override
	protected boolean isRelevantMetric(String metricName) {
		boolean isRelevant = _operatorConfiguration.get_filters().matchesPeConnectionMetricName(_domainId, _instanceId, _jobName, _peId, _connectionId, metricName);
		if (_trace.isInfoEnabled()) {
			if (isRelevant) {
				_trace.info("The following input port metric meets the filter criteria and is therefore, monitored: domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], peId=" + _peId + ", connectionId=" + _connectionId + ", metric=" + metricName);
			}
			else { 
				_trace.info("The following input port metric does not meet the filter criteria and is therefore, not monitored: domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], peId=" + _peId + ", connetionId=" + _connectionId + ", metric=" + metricName);
			}
		}
		return isRelevant;
	}

	@Override
	protected Set<Metric> retrieveMetrics() {
		Set<Metric> metrics = _connection.retrieveMetrics();
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
			_trace.debug("--> captureMetrics(domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "]:" + _jobName + ", peId=" + _peId + ", connectionId=" + _connectionId + ")");
		}

		_operatorConfiguration.get_tupleContainer().setOrigin("PeConnection");
		_operatorConfiguration.get_tupleContainer().setConnectionId(_connectionId);
		captureAndSubmitChangedMetrics();

		if (isDebugEnabled) {
			_trace.debug("<-- captureMetrics(domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "]:" + _jobName + ", peId=" + _peId + ", connectionId=" + _connectionId + ")");
		}
	}

}
