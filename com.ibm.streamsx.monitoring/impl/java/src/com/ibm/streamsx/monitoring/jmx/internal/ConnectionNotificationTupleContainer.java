//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.jmx.internal;

import java.util.HashMap;
import java.util.Map;
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

public class ConnectionNotificationTupleContainer {

	private final OperatorContext _context;	
	/**
	 * The output port.
	 */
	private StreamingOutput<OutputTuple> _port = null;

	/**
	 * Index of the eventTimestamp attribute.
	 */
	private Integer _eventTimestampAttributeIndex = null;	

	/**
	 * Index of the notifyType attribute.
	 */
	private Integer _notifyTypeAttributeIndex = null;	

	/**
	 * Index of the source attribute.
	 */
	private Integer _sourceAttributeIndex = null;
	
	/**
	 * Index of the message attribute.
	 */
	private Integer _messageAttributeIndex = null;

	/**
	 * Index of the sequence attribute.
	 */
	private Integer _sequenceAttributeIndex = null;

	
	/**
	 * Determine the indexes of output attributes and verify their types.
	 * 
	 * @param port
	 */
	public ConnectionNotificationTupleContainer(OperatorContext context, StreamingOutput<OutputTuple> port) {
		_context = context;
		// Create a tuple once.
		_port = port;
		
		StreamSchema schema = port.getStreamSchema();

		if (_eventTimestampAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("eventTimestamp");
			_eventTimestampAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.TIMESTAMP ? attribute.getIndex() : -1) ;
		}
		if (_notifyTypeAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("notifyType");
			_notifyTypeAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1) ;
		}
		if (_sourceAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("source");
			_sourceAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1) ;
		}		
		if (_messageAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("message");
			_messageAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1) ;
		}
		if (_sequenceAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("sequence");
			_sequenceAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.INT64 ? attribute.getIndex() : -1) ;
		}		
	}
	
	
	public void punctuate(Punctuation mark) throws Exception {
		_port.punctuate(mark);
	}
	
	/**
	 * Convert the notification to a tuple, using {@link #getAttributes(Notification, Object)}.
	 * @param notification Notification object.
	 * @return Tuple to be submitted.
	 */
	public Tuple getTuple(
			final Notification notification) {
		return _port.getStreamSchema().getTuple(getAttributes(notification));
	}
	
	/**
	 * Convert the notification to a {@code Map} of attributes keyed by attribute name.
	 * @param notification Notification object.
	 * @return Map containing attributes
	 */
	protected Map<String, Object> getAttributes(final Notification notification) {
		
		final Map<String, Object> attributes = new HashMap<String, Object>();
	
		if (_eventTimestampAttributeIndex != -1) {
			attributes.put("eventTimestamp", Timestamp.getTimestamp(notification.getTimeStamp()));
		}
		if (_notifyTypeAttributeIndex != -1) {
			attributes.put("notifyType", new RString(notification.getType()));
		}		
		if (_sequenceAttributeIndex != -1) {
			attributes.put("sequence", notification.getSequenceNumber());
		}
		if (_sourceAttributeIndex != -1) {
			Object source = notification.getSource();
			String ssource;
			if (source instanceof ObjectName) {
				ssource = ((ObjectName) source).getCanonicalName();
			}
			else {
				ssource = source.toString();
			}
			attributes.put("source", new RString(ssource));
		}
		if (_messageAttributeIndex != -1) {
			if (notification.getMessage() != null) {
				attributes.put("message", new RString(notification.getMessage()));
			}
		}
		
		return attributes;
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

}
