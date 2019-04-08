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

import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import javax.management.InstanceNotFoundException;
import javax.management.JMX;
import javax.management.ObjectName;
import com.ibm.streams.management.Notifications;
import com.ibm.streams.management.ObjectNameBuilder;
import com.ibm.streams.management.instance.InstanceMXBean;
import com.ibm.streams.management.job.JobMXBean;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.monitoring.jmx.OperatorConfiguration;

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
public class InstanceHandler implements NotificationListener, Closeable {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(InstanceHandler.class.getName());

	private OperatorConfiguration _operatorConfiguration = null;

	private String _instanceId = null;
	
	private ObjectName _objName = null;
	
	private InstanceMXBean _instance = null;

	private Map<String /* jobId */, JobHandler> _jobHandlers = new HashMap<>();

	public InstanceHandler(OperatorConfiguration operatorConfiguration, String instanceId) {

		boolean isDebugEnabled = _trace.isDebugEnabled();
		if (isDebugEnabled) {
			_trace.debug("InstanceHandler(" + instanceId + ")");
		}
		// Store parameters for later use.
		_operatorConfiguration = operatorConfiguration;
		_instanceId = instanceId;

		_objName = ObjectNameBuilder.instance(_instanceId);
		_instance = JMX.newMXBeanProxy(_operatorConfiguration.get_mbeanServerConnection(), _objName, InstanceMXBean.class, true);
		
		/*
		 * Register to get instance-related notifications.
		 */
		NotificationFilterSupport filter = new NotificationFilterSupport();
		filter.enableType(Notifications.JOB_ADDED);
		filter.enableType(Notifications.JOB_REMOVED);
		try {
			_operatorConfiguration.get_mbeanServerConnection().addNotificationListener(_objName, this, filter, null);
		} catch (InstanceNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
//	TODO      jmxc.addConnectionNotificationListener(this, null, null); // listen for potential lost notifications

		/*
		 * Register existing jobs.
		 */
		for(String jobId : _instance.getJobs()) {
			addValidJob(jobId);
		}
		
	}

	/**
	 * {@inheritDoc}
	 * 
	 * 
	 */
	@Override
	public void handleNotification(Notification notification, Object handback) {
		boolean isInfoEnabled = _trace.isInfoEnabled();

		if (notification.getType().equals(Notifications.JOB_ADDED)) {
			if(notification.getUserData() instanceof String) {
				/*
				 * Register existing jobs.
				 */
				String jobId = (String)notification.getUserData();
				
				if (null != _operatorConfiguration.get_tupleContainerJobStatusSource()) {
					final Tuple tuple = _operatorConfiguration.get_tupleContainerJobStatusSource().getTuple(notification, handback, _instanceId, jobId, null, null, null, null, null);
					_operatorConfiguration.get_tupleContainerJobStatusSource().submit(tuple);		
				}				
				
				addValidJob(jobId);
				if (isInfoEnabled) {
					_trace.info("received JOB_ADDED notification: jobId=" + jobId);
				}
			}
			else {
				_trace.error("received JOB_ADDED notification: user data is not an instance of BigInteger");
			}
		}
		else if (notification.getType().equals(Notifications.JOB_REMOVED)) {
			if(notification.getUserData() instanceof String) {
				/*
				 * Unregister existing jobs.
				 */
				String jobId = (String)notification.getUserData();
				if (_jobHandlers.containsKey(jobId)) {
					
					if (null != _operatorConfiguration.get_tupleContainerJobStatusSource()) {
						final Tuple tuple = _operatorConfiguration.get_tupleContainerJobStatusSource().getTuple(notification, handback, _instanceId, jobId, null, null, null, null, null);
						_operatorConfiguration.get_tupleContainerJobStatusSource().submit(tuple);
					}
					
					_jobHandlers.remove(jobId);
					if (isInfoEnabled) {
						_trace.info("received JOB_REMOVED notification for monitored job: jobId=" + jobId);
					}
				}
				else if (isInfoEnabled) {
					_trace.info("received JOB_REMOVED notification for job that is not monitored: jobId=" + jobId);
				}
			}
			else {
				_trace.error("received JOB_REMOVED notification: user data is not an instance of BigInteger");
			}
		}
		else {
			_trace.error("notification: " + notification + ", userData=" + notification.getUserData());
		}
	}

	protected void addValidJob(String jobId) {
		boolean isDebugEnabled = _trace.isDebugEnabled();
		if (isDebugEnabled) {
			_trace.debug("--> addValidJob(" + jobId + ")");
		}
		// Registering the job must be done before attempting to access any of
		// the job-related beans. 
		_instance.registerJob(jobId);
		// Special handling required because we do not have the job name easily accessible.
		ObjectName jobObjName = ObjectNameBuilder.job(_instanceId, jobId);
		JobMXBean job = JMX.newMXBeanProxy(_operatorConfiguration.get_mbeanServerConnection(), jobObjName, JobMXBean.class, true);
		String jobName = job.getName();
		boolean matches = _operatorConfiguration.get_filters().matchesJobName(_instanceId, jobName);
		if (_trace.isInfoEnabled()) {
			if (matches) {
				_trace.info("The following job meets the filter criteria and is therefore, monitored: instance=" + _instanceId + ", job=" + jobName + ", jobId=" + jobId);
			}
			else {
				_trace.info("The following job does not meet the filter criteria and is therefore, not monitored: instance=" + _instanceId + ", job=" + jobName + ", jobId=" + jobId);
			}
		}
		if (matches) {
			_jobHandlers.put(jobId, new JobHandler(_operatorConfiguration, _instanceId, jobId));
		}
		if (isDebugEnabled) {
			_trace.debug("<-- addValidJob(" + jobId + ")");
		}
	}
	
	/**
	 * Iterate all jobs to capture the job metrics.
	 * @throws Exception 
	 */
	public void captureMetrics() throws Exception {
		boolean isDebugEnabled = _trace.isDebugEnabled();
		if (isDebugEnabled) {
			_trace.debug("--> captureMetrics(instance=" + _instanceId + ")");
		}
		_operatorConfiguration.get_tupleContainerMetricsSource().setInstanceId(_instanceId);
		for(String jobId : _jobHandlers.keySet()) {
			_jobHandlers.get(jobId).captureMetrics();
		}
		if (isDebugEnabled) {
			_trace.debug("<-- captureMetrics(instance=" + _instanceId + ")");
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
		for(JobHandler handler : _jobHandlers.values()) {
			handler.close();
		}
		_jobHandlers.clear();
	}

	public void healthCheck() {
		if (_trace.isDebugEnabled()) {
			_trace.debug("healthCheck");
		}
		com.ibm.streams.management.instance.InstanceMXBean.Status status = _instance.getStatus();
		if (_trace.isDebugEnabled()) {
			_trace.debug("InstanceMXBean.Status="+status.toString());
		}
	}
	
}
