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

	private String _domainName = null;

	private String _instanceName = null;
	
	private InstanceMXBean _instance = null;

	private Map<BigInteger /* jobId */, JobHandler> _jobHandlers = new HashMap<BigInteger, JobHandler>();

	public InstanceHandler(OperatorConfiguration operatorConfiguration, String domainName, String instanceName) {

		if (_trace.isDebugEnabled()) {
			_trace.debug("InstanceHandler(" + domainName + "," + instanceName + ")");
		}
		// Store parameters for later use.
		_operatorConfiguration = operatorConfiguration;
		_domainName = domainName;
		_instanceName = instanceName;

		ObjectName instanceObjName = ObjectNameBuilder.instance(_domainName, _instanceName);
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
		_trace.error(notification);
		_trace.error(notification.getUserData());
		_trace.error(handback);
//		if (notification.getSequenceNumber())
		if (notification.getType() == Notifications.JOB_ADDED) {
			if(notification.getUserData() instanceof BigInteger) {
				/*
				 * Register existing jobs.
				 */
				BigInteger jobId = (BigInteger)notification.getUserData();
				addValidJob(jobId);
			}
			else {
				_trace.error("received JOB_ADDED notification: user data is not an instance of BigInteger");
			}
		}
		else if (notification.getType() == Notifications.JOB_REMOVED) {
			if(notification.getUserData() instanceof BigInteger) {
				/*
				 * Unregister existing jobs.
				 */
				BigInteger jobId = (BigInteger)notification.getUserData();
				if (_jobHandlers.containsKey(jobId)) {
					_jobHandlers.remove(jobId);
				}
			}
			else {
				_trace.error("received JOB_ADDED notification: user data is not an instance of BigInteger");
			}
		}
		else {
			_trace.error("notification: " + notification + ", userData=" + notification.getUserData());
		}
	}

	protected void addValidJob(BigInteger jobId) {
		// Special handling required because we do not have the job name easily accessible.
		ObjectName jobObjName = ObjectNameBuilder.job(_domainName, _instanceName, jobId);
		JobMXBean job = JMX.newMXBeanProxy(_operatorConfiguration.get_mbeanServerConnection(), jobObjName, JobMXBean.class, true);
		String jobName = job.getName();
		if(_operatorConfiguration.get_filters().matches(_domainName, _instanceName, jobName)) {
			_trace.error("The following job meets the filter criteria and is therefore, monitored: domain=" + _domainName + ", instance=" + _instanceName + ", job=" + jobName + ", jobId=" + jobId);
			_jobHandlers.put(jobId, new JobHandler(_operatorConfiguration, _domainName, _instanceName, jobId));
		}
		else { // TODO if (_trace.isInfoEnabled()) {
			_trace.error("The following job does not meet the filter criteria and is therefore, not monitored: domain=" + _domainName + ", instance=" + _instanceName + ", job=" + jobName + ", jobId=" + jobId);
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
		_operatorConfiguration.get_tupleContainer().setInstanceName(_instanceName);
		for(BigInteger jobId : _jobHandlers.keySet()) {
			_jobHandlers.get(jobId).captureMetrics();
		}
		if (_trace.isDebugEnabled()) {
			_trace.debug("<-- captureMetrics(domain=" + _domainName + ",instance=" + _instanceName + ")");
		}
	}

}
