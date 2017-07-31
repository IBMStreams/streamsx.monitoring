//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.notification;

import java.math.BigInteger;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Callable;

import javax.management.ObjectName;
import javax.management.Notification;
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.types.RString;
import com.ibm.streams.operator.types.Timestamp;

public class TupleContainer {

	private final OperatorContext _context;	
	private final StreamingOutput<?> _port;

	/**
	 * @param context
	 * @param port
	 */
	public TupleContainer(OperatorContext context, final StreamingOutput<?> port) {
		_port = port;
		_context = context;

	}

	public void punctuate(Punctuation mark) throws Exception {
		_port.punctuate(mark);
	}
	
	/**
	* Asynchronously submit the tuple to the output port.
	* @param tuple Tuple to be submitted.
	*/
	public void submit(final Tuple tuple) {
		_context.getScheduledExecutorService().submit(new Callable<Object>() {
			@Override
			public Object call() throws Exception {
				return _port.submit(tuple);
			}
		});        
	}	

	/**
	 * Convert the notification to a tuple, using {@link #getAttributes(Notification, Object)}.
	 * @param notification Notification object.
	 * @param handback Handback object.
	 * @return Tuple to be submitted.
	 */
	public Tuple getTuple(final Notification notification, final Object handback, final Object peHealth, final Object peStatus) {
		return _port.getStreamSchema().getTuple(getAttributes(notification, handback, peHealth, peStatus));
	}
	
	/**
	 * Convert the notification to a {@code Map} of attributes keyed by attribute name.
	 * @param notification Notification object.
	 * @param handback Handback object.
	 * @return Map containing attributes
	 */
	protected Map<String, Object> getAttributes(final Notification notification, final Object handback, final Object peHealth, final Object peStatus) {
		final Map<String, Object> attributes = new HashMap<String, Object>();
		Object source = notification.getSource();
		String ssource;
		if (source instanceof ObjectName)
			ssource = ((ObjectName) source).getCanonicalName();
		else
			ssource = source.toString();
	
		attributes.put("source", new RString(ssource));
		attributes.put("notifyType", new RString(notification.getType()));
		attributes.put("sequence", notification.getSequenceNumber());
		attributes.put("ts", Timestamp.getTimestamp(notification.getTimeStamp()));
		final Object userData = notification.getUserData();
		if (userData != null) {
			attributes.put("userData", userData);
			attributes.put("userDataString", new RString(userData.toString()));
		}
		if (handback != null)
			attributes.put("handback", handback);
		if (notification.getMessage() != null)
			attributes.put("message", new RString(notification.getMessage()));

		// additional PE health/status strings
		if (peHealth != null) {			
			attributes.put("peHealth", new RString(peHealth.toString()));
		}
		if (peStatus != null) {			
			attributes.put("peStatus", new RString(peStatus.toString()));
		}		
		return attributes;
	}	
}
