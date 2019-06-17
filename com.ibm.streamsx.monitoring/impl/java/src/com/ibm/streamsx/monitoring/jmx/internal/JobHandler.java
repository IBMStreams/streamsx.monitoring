//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.jmx.internal;

import javax.management.Notification;
import javax.management.NotificationFilterSupport;
import javax.management.NotificationListener;

import org.apache.log4j.Logger;

//import java.io.BufferedReader;
import java.io.IOException;
//import java.io.InputStream;
//import java.io.InputStreamReader;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

//import java.net.URL;
import javax.management.InstanceNotFoundException;
import javax.management.JMX;
import javax.management.ObjectName;
//import javax.net.ssl.HostnameVerifier;
//import javax.net.ssl.HttpsURLConnection;
//import javax.net.ssl.SSLContext;
//import javax.net.ssl.SSLSession;
//import javax.net.ssl.TrustManager;

import com.ibm.streams.management.Notifications;
import com.ibm.streams.management.ObjectNameBuilder;
import com.ibm.streams.management.job.JobMXBean;
import com.ibm.streamsx.monitoring.jmx.OperatorConfiguration;
import com.ibm.streamsx.monitoring.jmx.OperatorConfiguration.OpType;

/**
 * Listen for the following notifications:
 *
 * <ul>
 *   <li>com.ibm.streams.management.job.added
 *   <p>
 *   The Source operator registers with the job to receive
 *   job-related notifications.
 *   </p></li>
 *   <li>com.ibm.streams.management.job.removed
 *   <p>
 *   The Source operator removes all job-related information, so
 *   jobs of this instance are not monitored anymore.
 *   </p></li>
 * </ul> 
 */
public class JobHandler implements NotificationListener, Closeable {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(JobHandler.class.getName());

	private OperatorConfiguration _operatorConfiguration = null;

	private String _instanceId = null;

	private ObjectName _objName = null;
	
	private String _jobId = null;
	
	private String _jobName = null;

	private JobMXBean _job = null;

	private Map<String /* operatorName */, OperatorHandler> _operatorHandlers = new HashMap<>();

	private Map<String /* peId */, PeHandler> _peHandlers = new HashMap<>();

	public JobHandler(OperatorConfiguration applicationConfiguration, String instanceId, String jobId) {

		boolean isDebugEnabled = _trace.isDebugEnabled();
		if (isDebugEnabled) {
			_trace.debug("JobHandler(" + instanceId + "," + jobId + ")");
		}
		// Store parameters for later use.
		_operatorConfiguration = applicationConfiguration;
		_instanceId = instanceId;
		_jobId = jobId;

		_objName = ObjectNameBuilder.job(_instanceId, _jobId);
		_job = JMX.newMXBeanProxy(_operatorConfiguration.get_mbeanServerConnection(), _objName, JobMXBean.class, true);

		_jobName = _job.getName();

		/*
		 * Register to get job-related notifications.
		 */
		NotificationFilterSupport filter = new NotificationFilterSupport();
		filter.enableType(Notifications.INACTIVITY_WARNING);
		try {
			_operatorConfiguration.get_mbeanServerConnection().addNotificationListener(_objName, this, filter, null);
		} catch (InstanceNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		//	TODO      jmxc.addConnectionNotificationListener(this, null, null); // listen for potential lost notifications

		/*
		 * Create handlers for operators that match the filter criteria.
		 */
		if (OpType.METRICS_SOURCE == _operatorConfiguration.get_OperatorType()) {
			for(String operatorName : _job.getOperators()) {
				addValidOperator(operatorName);
			}
		}
		/*
		 * Create handlers for operators that match the filter criteria.
		 */
		for(String peId : _job.getPes()) {
			addPE(peId);
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
		if (notification.getType().equals(Notifications.INACTIVITY_WARNING)) {
			_job.keepRegistered();
		}
		else {
			_trace.error("notification: " + notification + ", userData=" + notification.getUserData());
		}
	}

	protected void addValidOperator(String operatorName) {
		boolean matches = _operatorConfiguration.get_filters().matchesOperatorName(_instanceId, _jobName, operatorName);
		if (_trace.isInfoEnabled()) {
			if (matches) {
				_trace.info("The following operator meets the filter criteria and is therefore, monitored: instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], operator=" + operatorName);
			}
			else { 
				_trace.info("The following operator does not meet the filter criteria and is therefore, not monitored: instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], operator=" + operatorName);
			}
		}
		if (matches) {
			_operatorHandlers.put(operatorName, new OperatorHandler(_operatorConfiguration, _instanceId, _jobId, _jobName, operatorName));
		}
	}
	
	protected void addPE(String peId) {
		boolean matches = _operatorConfiguration.get_filters().matchesPeId(_instanceId, _jobName, peId);
		if (_trace.isInfoEnabled()) {
			if (matches) {
				_trace.info("The following PE meets the filter criteria and is therefore, monitored: instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], peId=" + peId);
			}
			else { 
				_trace.info("The following PE does not meet the filter criteria and is therefore, not monitored: instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], peId=" + peId);
			}
		}
		if (matches) {
			_peHandlers.put(peId, new PeHandler(_operatorConfiguration, _instanceId, _jobId, _jobName, peId));
		}
	}
	
	/**
	 * Capture the job metrics.
	 * 
	 * Code taken from:
	 * http://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.0/com.ibm.streams.dev.doc/doc/jmxapi-lgop.html
	 * @throws Exception 
	 */
	public void captureMetrics() throws Exception {
		boolean isDebugEnabled = _trace.isDebugEnabled();
		if (isDebugEnabled) {
			_trace.debug("--> captureMetrics(instance=" + _instanceId + ",jobId=" + _jobId + ")");
		}
		MetricsTupleContainer schema = _operatorConfiguration.get_tupleContainerMetricsSource();
		schema.setJobId(_jobId);
		schema.setJobName(_jobName);
		for(String operatorName : _operatorHandlers.keySet()) {
			_operatorHandlers.get(operatorName).captureMetrics();
		}
		for(String peId : _peHandlers.keySet()) {
			_peHandlers.get(peId).captureMetrics();
		}
		if (isDebugEnabled) {
			_trace.debug("<-- captureMetrics(instance=" + _instanceId + ",jobId=" + _jobId + ")");
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
		for(OperatorHandler handler : _operatorHandlers.values()) {
			handler.close();
		}
		_peHandlers.clear();
		for(PeHandler handler : _peHandlers.values()) {
			handler.close();
		}
		_peHandlers.clear();
	}
	
	public String getJobName() {
		return _jobName;
	}
}
