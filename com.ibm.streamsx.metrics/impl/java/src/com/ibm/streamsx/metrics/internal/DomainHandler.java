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
import com.ibm.streams.management.domain.DomainMXBean;

/**
 * Listen for the following domain notifications:
 * 
 * <ul>
 *   <li>com.ibm.streams.management.instance.created
 *   <p>
 *   The MetricsSource operator registers with the instance to receive
 *   instance-related notifications.
 *   </p></li>
 *   <li>com.ibm.streams.management.instance.deleted
 *   <p>
 *   The MetricsSource operator removes all instance-related information, so
 *   jobs of this instance are not monitored anymore.
 *   </p></li>
 * </ul> 
 */
public class DomainHandler implements NotificationListener {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(DomainHandler.class.getName());

	private OperatorConfiguration _operatorConfiguration = null;

	private String _domainId = null;
	
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

		ObjectName domainObjName = ObjectNameBuilder.domain(_domainId);
		_domain = JMX.newMXBeanProxy(_operatorConfiguration.get_mbeanServerConnection(), domainObjName, DomainMXBean.class, true);

		/*
		 * Register to get domain-related notifications.
		 */
		NotificationFilterSupport filter = new NotificationFilterSupport();
		filter.enableType(Notifications.INSTANCE_CREATED);
		filter.enableType(Notifications.INSTANCE_DELETED);
		try {
			_operatorConfiguration.get_mbeanServerConnection().addNotificationListener(domainObjName, this, filter, null);
		} catch (InstanceNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
//	TODO      jmxc.addConnectionNotificationListener(this, null, null); // listen for potential lost notifications
		
		/*
		 * Register existing instances.
		 */
		for(String instanceId : _domain.getInstances()) {
			addValidInstance(instanceId);
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
		if (notification.getType().equals(Notifications.INSTANCE_CREATED)) {
			if(notification.getUserData() instanceof BigInteger) {
				/*
				 * Register instance.
				 */
				String instanceId = (String)notification.getUserData();
				addValidInstance(instanceId);
			}
			else {
				_trace.error("received INSTANCE_CREATED notification: user data is not an instance of String");
			}
		}
		else if (notification.getType().equals(Notifications.INSTANCE_DELETED)) {
			if(notification.getUserData() instanceof String) {
				/*
				 * Unregister existing instance.
				 */
				String instanceId = (String)notification.getUserData();
				if(_instanceHandlers.containsKey(instanceId)) {
					_instanceHandlers.remove(instanceId);
				}
			}
			else {
				_trace.error("received INSTANCE_DELETED notification: user data is not an instance of String");
			}
		}
		else {
			_trace.error("notification: " + notification + ", userData=" + notification.getUserData());
		}
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
		_operatorConfiguration.get_tupleContainer().setDomainId(_domainId);
		for(String instanceId : _instanceHandlers.keySet()) {
			_instanceHandlers.get(instanceId).captureMetrics();
		}
		if (isDebugEnabled) {
			_trace.debug("<-- captureMetrics(domain=" + _domainId + ")");
		}
	}

}
