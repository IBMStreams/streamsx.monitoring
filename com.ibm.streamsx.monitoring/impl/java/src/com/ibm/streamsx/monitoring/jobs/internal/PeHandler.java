//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.jobs.internal;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigInteger;

import javax.management.InstanceNotFoundException;
import javax.management.JMX;
import javax.management.Notification;
import javax.management.NotificationFilterSupport;
import javax.management.NotificationListener;
import javax.management.ObjectName;

import com.ibm.streams.management.Notifications;
import com.ibm.streams.management.ObjectNameBuilder;
import com.ibm.streams.management.job.PeMXBean;
import com.ibm.streams.operator.Tuple;


/**
 * 
 */
public class PeHandler implements NotificationListener, Closeable {

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(PeHandler.class.getName());

	private String _domainId = null;

	private String _instanceId = null;
	
	private BigInteger _jobId = null;
	
	private String _jobName = null;
	
	private ObjectName _objName = null;
	
	private BigInteger _peId = null;
	
	private PeMXBean _pe = null;
	
	private OperatorConfiguration _operatorConfiguration = null;

	public PeHandler(OperatorConfiguration operatorConfiguration, String domainId, String instanceId, BigInteger jobId, String jobName, BigInteger peId) {

		if (_trace.isDebugEnabled()) {
			_trace.debug("PeHandler(" + domainId + "," + instanceId + ")");
		}
		// Store parameters for later use.
		_operatorConfiguration = operatorConfiguration;
		_domainId = domainId;
		_instanceId = instanceId;
		_jobId = jobId;
		_jobName = jobName;
		_peId = peId;

		_objName = ObjectNameBuilder.pe(_domainId, _instanceId, _peId);
		_pe = JMX.newMXBeanProxy(_operatorConfiguration.get_mbeanServerConnection(), _objName, PeMXBean.class, true);
		
		/*
		 * Register to get pe-related notifications.
		 */
		NotificationFilterSupport filter = new NotificationFilterSupport();
//		filter.enableType(Notifications.PE_NOTIFICATION);
		filter.enableType(Notifications.PE_CHANGED);
		
		try {
			_operatorConfiguration.get_mbeanServerConnection().addNotificationListener(_objName, this, filter, null);
		} catch (InstanceNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		//	TODO      jmxc.addConnectionNotificationListener(this, null, null); // listen for potential lost notifications

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
		final Tuple tuple = _operatorConfiguration.get_tupleContainer().getTuple(notification, handback, _domainId, _instanceId, _jobId, _jobName, _pe.getResource(), _peId, _pe.getHealth(), _pe.getStatus());
		_operatorConfiguration.get_tupleContainer().submit(tuple);		

	}

	/**
	 * Remove notification listeners from this and child objects.
	 */
	@Override
	public void close() throws Exception {
		// Remove the notification listener.
		_operatorConfiguration.get_mbeanServerConnection().removeNotificationListener(_objName, this);
	}
	

}
