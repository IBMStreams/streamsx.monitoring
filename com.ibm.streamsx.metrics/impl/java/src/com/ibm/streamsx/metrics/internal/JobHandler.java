package com.ibm.streamsx.metrics.internal;

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
public class JobHandler implements NotificationListener {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(JobHandler.class.getName());

	private OperatorConfiguration _operatorConfiguration = null;

	private String _domainId = null;

	private String _instanceId = null;

	private BigInteger _jobId = null;
	
	private String _jobName = null;

	private JobMXBean _job = null;

	private Map<String /* operatorName */, OperatorHandler> _operatorHandlers = new HashMap<String, OperatorHandler>();

	public JobHandler(OperatorConfiguration applicationConfiguration, String domainId, String instanceId, BigInteger jobId) {

		boolean isDebugEnabled = _trace.isDebugEnabled();
		if (isDebugEnabled) {
			_trace.debug("JobHandler(" + domainId + "," + instanceId + "," + jobId + ")");
		}
		// Store parameters for later use.
		_operatorConfiguration = applicationConfiguration;
		_domainId = domainId;
		_instanceId = instanceId;
		_jobId = jobId;

		ObjectName jobObjName = ObjectNameBuilder.job(_domainId, _instanceId, _jobId);
		_job = JMX.newMXBeanProxy(_operatorConfiguration.get_mbeanServerConnection(), jobObjName, JobMXBean.class, true);

		_jobName = _job.getName();

		/*
		 * Register to get job-related notifications.
		 */
		NotificationFilterSupport filter = new NotificationFilterSupport();
		filter.enableType(Notifications.INACTIVITY_WARNING);
		try {
			_operatorConfiguration.get_mbeanServerConnection().addNotificationListener(jobObjName, this, filter, null);
		} catch (InstanceNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		//	TODO      jmxc.addConnectionNotificationListener(this, null, null); // listen for potential lost notifications

		/*
		 * Create handlers for operators that match the filter criteria.
		 */
		for(String operatorName : _job.getOperators()) {
			addValidOperator(operatorName);
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
		boolean matches = _operatorConfiguration.get_filters().matches(_domainId, _instanceId, _jobName, operatorName);
		if (_trace.isInfoEnabled()) {
			if (matches) {
				_trace.info("The following operator meets the filter criteria and is therefore, monitored: domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], operator=" + operatorName);
			}
			else { 
				_trace.info("The following operator does not meet the filter criteria and is therefore, not monitored: domain=" + _domainId + ", instance=" + _instanceId + ", job=[" + _jobId + "][" + _jobName + "], operator=" + operatorName);
			}
		}
		if (matches) {
			_operatorHandlers.put(operatorName, new OperatorHandler(_operatorConfiguration, _domainId, _instanceId, _jobId, _jobName, operatorName));
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
			_trace.debug("--> captureMetrics(domain=" + _domainId + ",instance=" + _instanceId + ",jobId=" + _jobId + ")");
		}
		TupleContainer schema = _operatorConfiguration.get_tupleContainer();
		schema.setJobId(_jobId);
		schema.setJobName(_jobName);
		for(String operatorName : _operatorHandlers.keySet()) {
			_operatorHandlers.get(operatorName).captureMetrics();
		}
		if (isDebugEnabled) {
			_trace.debug("<-- captureMetrics(domain=" + _domainId + ",instance=" + _instanceId + ",jobId=" + _jobId + ")");
		}
// -----------------------------------------------------------------------------
// SNAPSHOT METRICS CODE (BEGIN)
// -----------------------------------------------------------------------------
//		String uri = _job.snapshotMetrics();
//		try {
//			_trace.error("[" + _domainId + "," + _instanceId + "," + _jobName + "] capture metrics from URI: " + uri);
//
//			// TODO streamtool getdomainproperty jmx.sslOption
//			String sslOption = "TLSv1";
//			// set up trust manager to validate certificate issuer
//			TrustManager [] tm = new TrustManager[] {new JmxTrustManager()};
//			SSLContext ctxt = SSLContext.getInstance(sslOption);
//			ctxt.init(null, tm, null);
//
//			// set up host name verifier to trust the server from which the certificate was sent
//			HostnameVerifier hv = new HostnameVerifier() {
//				public boolean verify(String urlHostName, SSLSession session) {
//					// return false to reject
//					return true;
//				}
//			};
//
//			URL url = new URL(uri);
//			HttpsURLConnection conn = (HttpsURLConnection)url.openConnection();
//			conn.setSSLSocketFactory(ctxt.getSocketFactory());
//			conn.setHostnameVerifier(hv);
//			conn.setRequestMethod("GET");
//			conn.connect();
//			InputStream response = conn.getInputStream();
//			BufferedReader reader = new BufferedReader(new InputStreamReader(response));
//			String line;
//			while ((line = reader.readLine()) != null) {
//				System.out.println(line);
//			}
//			reader.close();
//			response.close();
//			conn.disconnect();
//
//			if (_jobIdAttributeIndex != -1) {
//				tuple.setLong(_jobIdAttributeIndex, _jobId.longValue());
//			}
//			if (_jobNameAttributeIndex != -1) {
//				tuple.setString(_jobNameAttributeIndex, _jobName);
//			}
//			// Submit tuple to output stream            
//			port.submit(tuple);
//
//		}
//		catch(Exception e) {
//			e.printStackTrace();
//		}
		// -----------------------------------------------------------------------------
		// SNAPSHOT METRICS CODE (END)
		// -----------------------------------------------------------------------------
	}

}
