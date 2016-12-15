package com.ibm.streamsx.metrics.internal;

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

/**
 * 
 */
public class OperatorHandler extends MetricOwningHandler implements NotificationListener {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(OperatorHandler.class.getName());

	private String _domainId = null;

	private String _instanceId = null;
	
	private BigInteger _jobId = null;
	
	private String _jobName = null;
	
	private String _operatorName = null;
	
	private OperatorMXBean _operator = null;

	private Map<Integer /* port index */, InputPortHandler> _inputPortHandlers = new HashMap<>();

	private Map<Integer /* port index */, OutputPortHandler> _outputPortHandlers = new HashMap<>();

	public OperatorHandler(OperatorConfiguration operatorConfiguration, String domainId, String instanceId, BigInteger jobId, String jobName, String operatorName) {

		super(MetricsRegistrationMode.DynamicMetricsRegistration);
		
		if (_trace.isDebugEnabled()) {
			_trace.debug("OperatorHandler(" + domainId + "," + instanceId + ")");
		}
		// Store parameters for later use.
		_operatorConfiguration = operatorConfiguration;
		_domainId = domainId;
		_instanceId = instanceId;
		_jobId = jobId;
		_jobName = jobName;
		_operatorName = operatorName;

		ObjectName operatorObjName = ObjectNameBuilder.operator(_domainId, _instanceId, _jobId, _operatorName);
		_operator = JMX.newMXBeanProxy(_operatorConfiguration.get_mbeanServerConnection(), operatorObjName, OperatorMXBean.class, true);
		
		/*
		 * Register to get job-related notifications.
		 */
		NotificationFilterSupport filter = new NotificationFilterSupport();
		filter.enableType(Notifications.OPERATOR_CONNECTION_ADDED);
		filter.enableType(Notifications.OPERATOR_CONNECTION_REMOVED);
		try {
			_operatorConfiguration.get_mbeanServerConnection().addNotificationListener(operatorObjName, this, filter, null);
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
			_trace.error("notification: " + notification + ", userData=" + notification.getUserData());
		}
		else if (notification.getType().equals(Notifications.OPERATOR_CONNECTION_REMOVED)) {
			/*
			 * userData is the connection's object name
			 */
			_trace.error("notification: " + notification + ", userData=" + notification.getUserData());
		}
		else {
			_trace.error("notification: " + notification + ", userData=" + notification.getUserData());
		}
	}

	@Override
	protected boolean isRelevantMetric(String metricName) {
		boolean isRelevant = _operatorConfiguration.get_filters().matches(_domainId, _instanceId, _jobName, _operatorName, metricName);
		if (_trace.isInfoEnabled()) {
			if (isRelevant) {
				_trace.info("The following operator custom metric meets the filter criteria and is therefore, monitored: domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], operator=" + _operatorName + ", metric=" + metricName);
			}
			else {
				_trace.info("The following operator custom metric does not meet the filter criteria and is therefore, not monitored: domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], operator=" + _operatorName + ", metric=" + metricName);
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
		// TODO Add port index to matches()
		boolean matches = _operatorConfiguration.get_filters().matches(_domainId, _instanceId, _jobName, _operatorName);
		if (_trace.isInfoEnabled()) {
			if (matches) {
				_trace.info("The following input port meets the filter criteria and is therefore, monitored: domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], operator=" + _operatorName + ", port=" + portIndex);
			}
			else {
				_trace.info("The following input port does not meet the filter criteria and is therefore, not monitored: domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], operator=" + _operatorName + ", port=" + portIndex);
			}
		}
		if (matches) {
			_inputPortHandlers.put(portIndex, new InputPortHandler(_operatorConfiguration, _domainId, _instanceId, _jobId, _jobName, _operatorName, portIndex));
		}
	}

	protected void addValidOutputPort(Integer portIndex) {
		// TODO Add port index to matches()
		boolean matches = _operatorConfiguration.get_filters().matches(_domainId, _instanceId, _jobName, _operatorName);
		if (_trace.isInfoEnabled()) {
			if (matches) {
				_trace.info("The following output port meets the filter criteria and is therefore, monitored: domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], operator=" + _operatorName + ", port=" + portIndex);
			}
			else {
				_trace.info("The following output port does not meet the filter criteria and is therefore, not monitored: domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], operator=" + _operatorName + ", port=" + portIndex);
			}
		}
		if (matches) {
			_outputPortHandlers.put(portIndex, new OutputPortHandler(_operatorConfiguration, _domainId, _instanceId, _jobId, _jobName, _operatorName, portIndex));
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
			_trace.debug("--> captureMetrics(domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "]:" + _jobName + ", operator=" + _operatorName + ")");
		}

		_operatorConfiguration.get_tupleContainer().setOperatorName(_operatorName);
		_operatorConfiguration.get_tupleContainer().setOrigin("Operator");
		_operatorConfiguration.get_tupleContainer().setPortIndex(0);

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
			_trace.debug("<-- captureMetrics(domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "]:" + _jobName + ", operator=" + _operatorName + ")");
		}
	}

}
