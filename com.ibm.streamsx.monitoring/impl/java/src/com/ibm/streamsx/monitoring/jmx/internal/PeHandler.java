//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.jmx.internal;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.JMX;
import javax.management.Notification;
import javax.management.NotificationFilterSupport;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import com.ibm.streams.management.Metric;
import com.ibm.streams.management.Notifications;
import com.ibm.streams.management.ObjectNameBuilder;
import com.ibm.streams.management.job.PeMXBean;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.monitoring.jmx.OperatorConfiguration;
import com.ibm.streamsx.monitoring.jmx.OperatorConfiguration.OpType;

/**
 * 
 */
public class PeHandler extends MetricOwningHandler implements NotificationListener, Closeable {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(PeHandler.class.getName());

	private String _domainId = null;

	private String _instanceId = null;
	
	private BigInteger _jobId = null;
	
	private String _jobName = null;
	
	private ObjectName _objName = null;
	
	private BigInteger _peId = null;
	
	private PeMXBean _pe = null;

	private Map<Integer /* port index */, PeInputPortHandler> _inputPortHandlers = new HashMap<>();

	private Map<Integer /* port index */, PeOutputPortHandler> _outputPortHandlers = new HashMap<>();
	
	private Map<String /* connection id */, PeConnectionHandler> _connectionHandlers = new HashMap<>();

	public PeHandler(OperatorConfiguration operatorConfiguration, String domainId, String instanceId, BigInteger jobId, String jobName, BigInteger peId) {
		super(MetricsRegistrationMode.DynamicMetricsRegistration);
		
		if (_trace.isDebugEnabled()) {
			_trace.debug("PeHandler(" + domainId + "," + instanceId + ")");
		}
		// Store parameters for later use.
		_operatorConfiguration = operatorConfiguration;
		_domainId = domainId;
		_instanceId = instanceId;
		_jobId = jobId;
		_jobName = jobName;
		_peId = peId;

		_objName = ObjectNameBuilder.pe(_domainId, _instanceId, _peId);
		_pe = JMX.newMXBeanProxy(_operatorConfiguration.get_mbeanServerConnection(), _objName, PeMXBean.class, true);
		
		/*
		 * Register to get pe-related notifications.
		 */
		NotificationFilterSupport filter = new NotificationFilterSupport();
		if (OpType.JOB_STATUS_SOURCE == _operatorConfiguration.get_OperatorType()) {
			filter.enableType(Notifications.PE_CHANGED);
		}
		else {
			filter.enableType(Notifications.PE_NOTIFICATION);
		}
		try {
			_operatorConfiguration.get_mbeanServerConnection().addNotificationListener(_objName, this, filter, null);
		} catch (InstanceNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		//	TODO      jmxc.addConnectionNotificationListener(this, null, null); // listen for potential lost notifications
		if (OpType.METRICS_SOURCE == _operatorConfiguration.get_OperatorType()) {
			/*
			 * Register input port metrics that match the specified filter criteria.
			 */
			registerMetrics();
			
			/*
			 * Register input port metrics that match the specified filter criteria.
			 */
			for (Integer portIndex : _pe.getInputPorts()) {
				addValidInputPort(portIndex);
			}
			/*
			 * Register output port metrics that match the specified filter criteria.
			 */
			for (Integer portIndex : _pe.getOutputPorts()) {
				addValidOutputPort(portIndex);
			}
			
			for (String connectionId : _pe.getConnections()) {
				addValidConnection(connectionId);
			}
		}
	}

	/**
	 * {@inheritDoc}
	 * 
	 * 
	 */
	@Override
	public void handleNotification(Notification notification, Object handback) {
		if (_trace.isDebugEnabled()) {
			_trace.debug("notification: " + notification + ", userData=" + notification.getUserData());
		}
		if (OpType.JOB_STATUS_SOURCE == _operatorConfiguration.get_OperatorType()) {
			final Tuple tuple = _operatorConfiguration.get_tupleContainerJobStatusSource().getTuple(notification, handback, _domainId, _instanceId, _jobId, _jobName, _pe.getResource(), _peId, _pe.getHealth(), _pe.getStatus());
			_operatorConfiguration.get_tupleContainerJobStatusSource().submit(tuple);
		}
	}

	@Override
	protected boolean isRelevantMetric(String metricName) {
		boolean isRelevant = _operatorConfiguration.get_filters().matchesPeMetricName(_domainId, _instanceId, _jobName, _peId, metricName);
		if (_trace.isInfoEnabled()) {
			if (isRelevant) {
				_trace.info("The following pe metric meets the filter criteria and is therefore, monitored: domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], peId=" + _peId + ", metric=" + metricName);
			}
			else {
				_trace.info("The following pe metric does not meet the filter criteria and is therefore, not monitored: domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], peId=" + _peId + ", metric=" + metricName);
			}
		}
		return isRelevant;
	}

	@Override
	protected Set<Metric> retrieveMetrics() {
		Set<Metric> metrics = _pe.retrieveMetrics(false);
		return metrics;
	}

	protected void addValidInputPort(Integer portIndex) {
		boolean matches = _operatorConfiguration.get_filters().matchesPeInputPortIndex(_domainId, _instanceId, _jobName, _peId, portIndex);
		if (_trace.isInfoEnabled()) {
			if (matches) {
				_trace.info("The following input port meets the filter criteria and is therefore, monitored: domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], peId=" + _peId + ", port=" + portIndex);
			}
			else {
				_trace.info("The following input port does not meet the filter criteria and is therefore, not monitored: domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], peId=" + _peId + ", port=" + portIndex);
			}
		}
		if (matches) {
			_inputPortHandlers.put(portIndex, new PeInputPortHandler(_operatorConfiguration, _domainId, _instanceId, _jobId, _jobName, _peId, portIndex));
		}
	}

	protected void addValidOutputPort(Integer portIndex) {
		boolean matches = _operatorConfiguration.get_filters().matchesPeOutputPortIndex(_domainId, _instanceId, _jobName, _peId, portIndex);
		if (_trace.isInfoEnabled()) {
			if (matches) {
				_trace.info("The following output port meets the filter criteria and is therefore, monitored: domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], peId=" + _peId + ", port=" + portIndex);
			}
			else {
				_trace.info("The following output port does not meet the filter criteria and is therefore, not monitored: domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], peId=" + _peId + ", port=" + portIndex);
			}
		}
		if (matches) {
			_outputPortHandlers.put(portIndex, new PeOutputPortHandler(_operatorConfiguration, _domainId, _instanceId, _jobId, _jobName, _peId, portIndex));
		}
	}
	
	protected void addValidConnection(String connectionId) {
		boolean matches = _operatorConfiguration.get_filters().matchesPeConnectionId(_domainId, _instanceId, _jobName, _peId, connectionId);
		if (_trace.isInfoEnabled()) {
			if (matches) {
				_trace.info("The following output port meets the filter criteria and is therefore, monitored: domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], peId=" + _peId + ", connection=" + connectionId);
			}
			else {
				_trace.info("The following output port does not meet the filter criteria and is therefore, not monitored: domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], peId=" + _peId + ", connection=" + connectionId);
			}
		}
		if (matches) {
			_connectionHandlers.put(connectionId, new PeConnectionHandler(_operatorConfiguration, _domainId, _instanceId, _jobId, _jobName, _peId, connectionId));
		}
	}

	/**
	 * Iterate all jobs to capture the job metrics.
	 * 
	 * @throws Exception
	 * Throws Exception if submitting the tuple failed. 
	 */
	public void captureMetrics() throws Exception {
		// Determine the trace level status once per function.
		boolean isDebugEnabled = _trace.isDebugEnabled();

		if (isDebugEnabled) {
			_trace.debug("--> captureMetrics(domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "]:" + _jobName + ", peId=" + _peId + ")");
		}
		MetricsTupleContainer tc = _operatorConfiguration.get_tupleContainerMetricsSource();
		tc.setOperatorName("");
		tc.setOrigin("Pe");
		tc.setPortIndex(0);
		tc.setChannel(-1);
		tc.setPeId(_peId);
		tc.setResource(_pe.getResource());

		captureAndSubmitChangedMetrics();

		/*
		 * Capture port metrics.
		 */
		for(Integer portIndex : _inputPortHandlers.keySet()) {
			_inputPortHandlers.get(portIndex).captureMetrics();
		}
		for(Integer portIndex : _outputPortHandlers.keySet()) {
			_outputPortHandlers.get(portIndex).captureMetrics();
		}
		for(String connectionId : _connectionHandlers.keySet()) {
			_connectionHandlers.get(connectionId).captureMetrics();
		}

		if (isDebugEnabled) {
			_trace.debug("<-- captureMetrics(domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "]:" + _jobName + ", peId=" + _peId + ")");
		}
	}

	/**
	 * Remove notification listeners from this and child objects.
	 */
	@Override
	public void close() throws Exception {
		// Remove the notification listener.
		_operatorConfiguration.get_mbeanServerConnection().removeNotificationListener(_objName, this);
		// Close all resources of all child objects.
		for(PeInputPortHandler handler : _inputPortHandlers.values()) {
			handler.close();
		}
		_inputPortHandlers.clear();
		for(PeOutputPortHandler handler : _outputPortHandlers.values()) {
			handler.close();
		}
		_outputPortHandlers.clear();
	}

}
