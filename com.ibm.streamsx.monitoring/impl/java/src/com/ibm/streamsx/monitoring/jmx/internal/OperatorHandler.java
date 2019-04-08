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
import com.ibm.streams.management.job.OperatorMXBean;
import com.ibm.streamsx.monitoring.jmx.OperatorConfiguration;

/**
 * 
 */
public class OperatorHandler extends MetricOwningHandler implements NotificationListener, Closeable {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(OperatorHandler.class.getName());

	private String _instanceId = null;
	
	private BigInteger _jobId = null;
	
	private String _jobName = null;
	
	private ObjectName _objName = null;
	
	private String _operatorName = null;
	
	private OperatorMXBean _operator = null;

	private Map<Integer /* port index */, OperatorInputPortHandler> _inputPortHandlers = new HashMap<>();

	private Map<Integer /* port index */, OperatorOutputPortHandler> _outputPortHandlers = new HashMap<>();

	public OperatorHandler(OperatorConfiguration operatorConfiguration, String instanceId, BigInteger jobId, String jobName, String operatorName) {

		super(MetricsRegistrationMode.DynamicMetricsRegistration);
		
		if (_trace.isDebugEnabled()) {
			_trace.debug("OperatorHandler(" + instanceId + ")");
		}
		// Store parameters for later use.
		_operatorConfiguration = operatorConfiguration;
		_instanceId = instanceId;
		_jobId = jobId;
		_jobName = jobName;
		_operatorName = operatorName;

		_objName = ObjectNameBuilder.operator(_instanceId, _jobId, _operatorName);
		_operator = JMX.newMXBeanProxy(_operatorConfiguration.get_mbeanServerConnection(), _objName, OperatorMXBean.class, true);
		
		/*
		 * Register to get job-related notifications.
		 */
		NotificationFilterSupport filter = new NotificationFilterSupport();
		filter.enableType(Notifications.OPERATOR_CONNECTION_ADDED);
		filter.enableType(Notifications.OPERATOR_CONNECTION_REMOVED);
		try {
			_operatorConfiguration.get_mbeanServerConnection().addNotificationListener(_objName, this, filter, null);
		} catch (InstanceNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		//	TODO      jmxc.addConnectionNotificationListener(this, null, null); // listen for potential lost notifications

		/*
		 * Register input port metrics that match the specified filter criteria.
		 */
		registerMetrics();
		
		/*
		 * Register input port metrics that match the specified filter criteria.
		 */
		for (Integer portIndex : _operator.getInputPorts()) {
			addValidInputPort(portIndex);
		}
		/*
		 * Register output port metrics that match the specified filter criteria.
		 */
		for (Integer portIndex : _operator.getOutputPorts()) {
			addValidOutputPort(portIndex);
		}
	}

	/**
	 * {@inheritDoc}
	 * 
	 * 
	 */
	@Override
	public void handleNotification(Notification notification, Object handback) {
		//		if (notification.getSequenceNumber())
		if (notification.getType().equals(Notifications.OPERATOR_CONNECTION_ADDED)) {
			/*
			 * userData is the connection's object name
			 */
			if (_trace.isDebugEnabled()) {
				_trace.debug("notification: " + notification + ", userData=" + notification.getUserData());
			}
		}
		else if (notification.getType().equals(Notifications.OPERATOR_CONNECTION_REMOVED)) {
			/*
			 * userData is the connection's object name
			 */
			if (_trace.isDebugEnabled()) {
				_trace.debug("notification: " + notification + ", userData=" + notification.getUserData());
			}
		}
		else {
			if (_trace.isDebugEnabled()) {
				_trace.debug("notification: " + notification + ", userData=" + notification.getUserData());
			}
		}
	}

	@Override
	protected boolean isRelevantMetric(String metricName) {
		boolean isRelevant = _operatorConfiguration.get_filters().matchesOperatorMetricName(_instanceId, _jobName, _operatorName, metricName);
		if (_trace.isInfoEnabled()) {
			if (isRelevant) {
				_trace.info("The following operator custom metric meets the filter criteria and is therefore, monitored: instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], operator=" + _operatorName + ", metric=" + metricName);
			}
			else {
				_trace.info("The following operator custom metric does not meet the filter criteria and is therefore, not monitored: instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], operator=" + _operatorName + ", metric=" + metricName);
			}
		}
		return isRelevant;
	}

	@Override
	protected Set<Metric> retrieveMetrics() {
		Set<Metric> metrics = _operator.retrieveMetrics(false);
		return metrics;
	}

	protected void addValidInputPort(Integer portIndex) {
		boolean matches = _operatorConfiguration.get_filters().matchesOperatorInputPortIndex(_instanceId, _jobName, _operatorName, portIndex);
		if (_trace.isInfoEnabled()) {
			if (matches) {
				_trace.info("The following input port meets the filter criteria and is therefore, monitored: instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], operator=" + _operatorName + ", port=" + portIndex);
			}
			else {
				_trace.info("The following input port does not meet the filter criteria and is therefore, not monitored: instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], operator=" + _operatorName + ", port=" + portIndex);
			}
		}
		if (matches) {
			_inputPortHandlers.put(portIndex, new OperatorInputPortHandler(_operatorConfiguration, _instanceId, _jobId, _jobName, _operatorName, portIndex));
		}
	}

	protected void addValidOutputPort(Integer portIndex) {
		boolean matches = _operatorConfiguration.get_filters().matchesOperatorOutputPortIndex(_instanceId, _jobName, _operatorName, portIndex);
		if (_trace.isInfoEnabled()) {
			if (matches) {
				_trace.info("The following output port meets the filter criteria and is therefore, monitored: instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], operator=" + _operatorName + ", port=" + portIndex);
			}
			else {
				_trace.info("The following output port does not meet the filter criteria and is therefore, not monitored: instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], operator=" + _operatorName + ", port=" + portIndex);
			}
		}
		if (matches) {
			_outputPortHandlers.put(portIndex, new OperatorOutputPortHandler(_operatorConfiguration, _instanceId, _jobId, _jobName, _operatorName, portIndex));
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
			_trace.debug("--> captureMetrics(instance=" + _instanceId + ", job=[" + _jobId + "]:" + _jobName + ", operator=" + _operatorName + ")");
		}

		MetricsTupleContainer tc = _operatorConfiguration.get_tupleContainerMetricsSource();
		tc.setOperatorName(_operatorName);
		tc.setOrigin("Operator");
		tc.setPortIndex(0);
		tc.setChannel(_operator.getChannel());
		tc.setPeId(_operator.getPe());
		tc.setResource(_operator.getResource());

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

		if (isDebugEnabled) {
			_trace.debug("<-- captureMetrics(instance=" + _instanceId + ", job=[" + _jobId + "]:" + _jobName + ", operator=" + _operatorName + ")");
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
		for(OperatorInputPortHandler handler : _inputPortHandlers.values()) {
			handler.close();
		}
		_inputPortHandlers.clear();
		for(OperatorOutputPortHandler handler : _outputPortHandlers.values()) {
			handler.close();
		}
		_outputPortHandlers.clear();
	}

}
