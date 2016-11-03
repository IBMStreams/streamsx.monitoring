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

	private String _domainName = null;
	
	private DomainMXBean _domain = null;
	
	private Map<String /* instanceName */, InstanceHandler> _instanceHandlers = new HashMap<String, InstanceHandler>();

	public DomainHandler(OperatorConfiguration operatorConfiguration, String domainName) {

		if (_trace.isDebugEnabled()) {
			_trace.debug("DomainHandler(" + domainName + ")");
		}
		
		// Store parameters for later use.
		_operatorConfiguration = operatorConfiguration;
		_domainName = domainName;

		ObjectName domainObjName = ObjectNameBuilder.domain(_domainName);
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
		for(String instanceName : _domain.getInstances()) {
			addValidInstance(instanceName);
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
				String instanceName = (String)notification.getUserData();
				addValidInstance(instanceName);
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
				String instanceName = (String)notification.getUserData();
				if(_instanceHandlers.containsKey(instanceName)) {
					_instanceHandlers.remove(instanceName);
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

	protected void addValidInstance(String instanceName) {
		if(_operatorConfiguration.get_filters().matches(_domainName, instanceName)) {
			_trace.error("The following instance meets the filter criteria and is therefore, monitored: domain=" + _domainName + ", instance=" + instanceName);
			_instanceHandlers.put(instanceName, new InstanceHandler(_operatorConfiguration, _domainName, instanceName));
		}
		else { // TODO if (_trace.isInfoEnabled()) {
			_trace.error("The following instance does not meet the filter criteria and is therefore, not monitored: domain=" + _domainName + ", instance=" + instanceName);
		}
	}
	
	/**
	 * Iterate all instances to capture the job metrics.
	 * @throws Exception 
	 */
	public void captureMetrics() throws Exception {
		if (_trace.isDebugEnabled()) {
			_trace.debug("--> captureMetrics(domain=" + _domainName + ")");
		}
		_operatorConfiguration.get_tupleContainer().setDomainName(_domainName);
		for(String instanceName : _instanceHandlers.keySet()) {
			_instanceHandlers.get(instanceName).captureMetrics();
		}
		if (_trace.isDebugEnabled()) {
			_trace.debug("<-- captureMetrics(domain=" + _domainName + ")");
		}
	}

}
