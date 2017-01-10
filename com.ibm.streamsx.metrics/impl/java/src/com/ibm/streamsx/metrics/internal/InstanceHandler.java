//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.metrics.internal;

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

/**
 * Listen for the following domain notifications:
 *
 * <ul>
 *   <li>com.ibm.streams.management.job.added
 *   <p>
 *   The MetricsSource operator registers with the job to receive
 *   job-related notifications.
 *   </p></li>
 *   <li>com.ibm.streams.management.job.removed
 *   <p>
 *   The MetricsSource operator removes all job-related information, so
 *   jobs of this instance are not monitored anymore.
 *   </p></li>
 * </ul> 
 */
public class InstanceHandler implements NotificationListener {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(InstanceHandler.class.getName());

	private OperatorConfiguration _operatorConfiguration = null;

	private String _domainId = null;

	private String _instanceId = null;
	
	private InstanceMXBean _instance = null;

	private Map<BigInteger /* jobId */, JobHandler> _jobHandlers = new HashMap<>();

	public InstanceHandler(OperatorConfiguration operatorConfiguration, String domainId, String instanceId) {

		boolean isDebugEnabled = _trace.isDebugEnabled();
		if (isDebugEnabled) {
			_trace.debug("InstanceHandler(" + domainId + "," + instanceId + ")");
		}
		// Store parameters for later use.
		_operatorConfiguration = operatorConfiguration;
		_domainId = domainId;
		_instanceId = instanceId;

		ObjectName instanceObjName = ObjectNameBuilder.instance(_domainId, _instanceId);
		_instance = JMX.newMXBeanProxy(_operatorConfiguration.get_mbeanServerConnection(), instanceObjName, InstanceMXBean.class, true);
		
		/*
		 * Register to get instance-related notifications.
		 */
		NotificationFilterSupport filter = new NotificationFilterSupport();
		filter.enableType(Notifications.JOB_ADDED);
		filter.enableType(Notifications.JOB_REMOVED);
		try {
			_operatorConfiguration.get_mbeanServerConnection().addNotificationListener(instanceObjName, this, filter, null);
		} catch (InstanceNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
//	TODO      jmxc.addConnectionNotificationListener(this, null, null); // listen for potential lost notifications

		/*
		 * Register existing jobs.
		 */
		for(BigInteger jobId : _instance.getJobs()) {
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
//		if (notification.getSequenceNumber())
		if (notification.getType().equals(Notifications.JOB_ADDED)) {
			if(notification.getUserData() instanceof BigInteger) {
				/*
				 * Register existing jobs.
				 */
				BigInteger jobId = (BigInteger)notification.getUserData();
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
			if(notification.getUserData() instanceof BigInteger) {
				/*
				 * Unregister existing jobs.
				 */
				BigInteger jobId = (BigInteger)notification.getUserData();
				if (_jobHandlers.containsKey(jobId)) {
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

	protected void addValidJob(BigInteger jobId) {
		boolean isDebugEnabled = _trace.isDebugEnabled();
		if (isDebugEnabled) {
			_trace.debug("--> addValidJob(" + jobId + ")");
		}
		// Registering the job must be done before attempting to access any of
		// the job-related beans. 
		_instance.registerJob(jobId);
		// Special handling required because we do not have the job name easily accessible.
		ObjectName jobObjName = ObjectNameBuilder.job(_domainId, _instanceId, jobId);
		JobMXBean job = JMX.newMXBeanProxy(_operatorConfiguration.get_mbeanServerConnection(), jobObjName, JobMXBean.class, true);
		String jobName = job.getName();
		boolean matches = _operatorConfiguration.get_filters().matchesJobName(_domainId, _instanceId, jobName);
		if (_trace.isInfoEnabled()) {
			if (matches) {
				_trace.info("The following job meets the filter criteria and is therefore, monitored: domain=" + _domainId + ", instance=" + _instanceId + ", job=" + jobName + ", jobId=" + jobId);
			}
			else {
				_trace.info("The following job does not meet the filter criteria and is therefore, not monitored: domain=" + _domainId + ", instance=" + _instanceId + ", job=" + jobName + ", jobId=" + jobId);
			}
		}
		if (matches) {
			_jobHandlers.put(jobId, new JobHandler(_operatorConfiguration, _domainId, _instanceId, jobId));
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
			_trace.debug("--> captureMetrics(domain=" + _domainId + ",instance=" + _instanceId + ")");
		}
		_operatorConfiguration.get_tupleContainer().setInstanceId(_instanceId);
		for(BigInteger jobId : _jobHandlers.keySet()) {
			_jobHandlers.get(jobId).captureMetrics();
		}
		if (isDebugEnabled) {
			_trace.debug("<-- captureMetrics(domain=" + _domainId + ",instance=" + _instanceId + ")");
		}
	}

}
