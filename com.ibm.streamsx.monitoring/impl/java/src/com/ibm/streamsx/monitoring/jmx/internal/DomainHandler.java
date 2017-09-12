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

import com.ibm.json.java.JSON;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.management.Notifications;
import com.ibm.streams.management.ObjectNameBuilder;
import com.ibm.streams.management.domain.DomainMXBean;
import com.ibm.streams.operator.Tuple;
import com.ibm.streamsx.monitoring.jmx.OperatorConfiguration;
import com.ibm.streamsx.monitoring.jmx.OperatorConfiguration.OpType;

/**
 * Listen for the following domain notifications:
 * 
 * <ul>
 *   <li>com.ibm.streams.management.instance.created
 *   <p>
 *   The Source operator registers with the instance to receive
 *   instance-related notifications.
 *   </p></li>
 *   <li>com.ibm.streams.management.instance.deleted
 *   <p>
 *   The Source operator removes all instance-related information, so
 *   jobs of this instance are not monitored anymore.
 *   </p></li>
 * </ul> 
 */
public class DomainHandler implements NotificationListener, Closeable {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(DomainHandler.class.getName());

	private OperatorConfiguration _operatorConfiguration = null;

	private String _domainId = null;
	
	private ObjectName _objName = null;
	
	private DomainMXBean _domain = null;
	
	private Map<String /* instanceId */, InstanceHandler> _instanceHandlers = new HashMap<>();

	public DomainHandler(OperatorConfiguration operatorConfiguration, String domainId) {

		boolean isDebugEnabled = _trace.isDebugEnabled();
		if (isDebugEnabled) {
			_trace.debug("DomainHandler(" + domainId + ")");
		}
		
		// Store parameters for later use.
		_operatorConfiguration = operatorConfiguration;
		_domainId = domainId;

		_objName = ObjectNameBuilder.domain(_domainId);
		_domain = JMX.newMXBeanProxy(_operatorConfiguration.get_mbeanServerConnection(), _objName, DomainMXBean.class, true);

		/*
		 * Register to get domain-related notifications.
		 */
		NotificationFilterSupport filter = new NotificationFilterSupport();
		if (OpType.LOG_SOURCE == _operatorConfiguration.get_OperatorType()) {
			filter.enableType("com.ibm.streams.management.log.application.error");
			filter.enableType("com.ibm.streams.management.log.application.warning");		
		}
		else {
			filter.enableType(Notifications.INSTANCE_CREATED);
			filter.enableType(Notifications.INSTANCE_DELETED);
		}
		try {
			_operatorConfiguration.get_mbeanServerConnection().addNotificationListener(_objName, this, filter, null);
		} catch (InstanceNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
//	TODO      jmxc.addConnectionNotificationListener(this, null, null); // listen for potential lost notifications
		
		if (OpType.LOG_SOURCE != _operatorConfiguration.get_OperatorType()) {
			/*
			 * Register existing instances.
			 */
			for(String instanceId : _domain.getInstances()) {
				addValidInstance(instanceId);
			}
		}
		
	}
	
	public void healthCheck() {
		if (_trace.isDebugEnabled()) {
			_trace.debug("healthCheck");
		}
		com.ibm.streams.management.domain.DomainMXBean.Status status = _domain.getStatus();
		if (_trace.isDebugEnabled()) {
			_trace.debug("DomainMXBean.Status="+status.toString());
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
		if (notification.getType().equals(Notifications.INSTANCE_CREATED)) {
			String instanceId = getInstanceFromUserData(notification.getUserData());
			if (instanceId != "") {
				/*
				 * Register instance.
				 */
				addValidInstance(instanceId);
			}
			else {
				_trace.error("received INSTANCE_CREATED notification: user data does not contain instance name");
			}
		}
		else if (notification.getType().equals(Notifications.INSTANCE_DELETED)) {
			String instanceId = getInstanceFromUserData(notification.getUserData());
			if (instanceId != "") {
				/*
				 * Unregister existing instance.
				 */
				if(_instanceHandlers.containsKey(instanceId)) {
					_instanceHandlers.remove(instanceId);
					if (_trace.isInfoEnabled()) {
						_trace.info("The following instance is deleted: domain=" + _domainId + ", instance=" + instanceId);
					}
				}
			}
			else {
				_trace.error("received INSTANCE_DELETED notification: user data does not contain instance name");
			}
		}
		else {
			if ((OpType.LOG_SOURCE == _operatorConfiguration.get_OperatorType()) &&
				(notification.getType().contains("com.ibm.streams.management.log.application"))) {
				// emit tuple
				try {
					JSONObject obj = (JSONObject)JSON.parse(notification.getUserData().toString());
					String instance = obj.get("instance").toString();
					String resource = obj.get("resource").toString();
					BigInteger pe = new BigInteger(obj.get("pe").toString());
					BigInteger job = new BigInteger(obj.get("job").toString());
					String operator = obj.get("operator").toString();

					final Tuple tuple = _operatorConfiguration.get_tupleContainerLogSource().getTuple(notification, _domainId, instance, resource, pe, job, operator);
					_operatorConfiguration.get_tupleContainerLogSource().submit(tuple);
				}
				catch (Exception e) {
					_trace.error("Error in parsing userData: " + e);
				}
			}
			else {
				_trace.error("notification: " + notification + ", userData=" + notification.getUserData());
			}
		}
	}

	private String getInstanceFromUserData(Object userDataObj) {		
		String result = "";
		if (null != userDataObj) {
			String userData = userDataObj.toString();
			// com.ibm.streams.management:type=domain.instance,domain="xxx",name="yyy"
			int idx = userData.lastIndexOf("name=");
			if ((idx > -1) && (idx+6 < userData.length())) {
				result = userData.substring(idx+6, userData.length()-1);
			}
		}
		return result;
	}
	
	protected void addValidInstance(String instanceId) {
		boolean matches = _operatorConfiguration.get_filters().matchesInstanceId(_domainId, instanceId);
		if (_trace.isInfoEnabled()) {
			if (matches) {
				_trace.info("The following instance meets the filter criteria and is therefore, monitored: domain=" + _domainId + ", instance=" + instanceId);
			}
			else {
				_trace.info("The following instance does not meet the filter criteria and is therefore, not monitored: domain=" + _domainId + ", instance=" + instanceId);
			}
		}
		if(matches) {
			_instanceHandlers.put(instanceId, new InstanceHandler(_operatorConfiguration, _domainId, instanceId));
		}
	}
	
	/**
	 * Iterate all instances to capture the job metrics.
	 * @throws Exception 
	 */
	public void captureMetrics() throws Exception {
		boolean isDebugEnabled = _trace.isDebugEnabled();
		if (isDebugEnabled) {
			_trace.debug("--> captureMetrics(domain=" + _domainId + ")");
		}
		_operatorConfiguration.get_tupleContainerMetricsSource().setDomainId(_domainId);
		for(String instanceId : _instanceHandlers.keySet()) {
			_instanceHandlers.get(instanceId).captureMetrics();
		}
		if (isDebugEnabled) {
			_trace.debug("<-- captureMetrics(domain=" + _domainId + ")");
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
		for(InstanceHandler handler : _instanceHandlers.values()) {
			handler.close();
		}
		_instanceHandlers.clear();
	}

}
