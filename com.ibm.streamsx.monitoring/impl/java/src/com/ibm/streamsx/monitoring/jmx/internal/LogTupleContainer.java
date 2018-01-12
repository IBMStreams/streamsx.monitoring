//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.jmx.internal;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

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

public class LogTupleContainer {

	private final OperatorContext _context;	
	/**
	 * The output port.
	 */
	private StreamingOutput<OutputTuple> _port = null;

	/**
	 * Index of the domainId attribute.
	 */
	private Integer _domainIdAttributeIndex = null;
	
	/**
	 * Index of the instanceId attribute.
	 */
	private Integer _instanceIdAttributeIndex = null;

	/**
	 * Index of the jobId attribute.
	 */
	private Integer _jobIdAttributeIndex = null;

	/**
	 * Index of the resource attribute.
	 */
	private Integer _resourceAttributeIndex = null;

	/**
	 * Index of the peId attribute.
	 */
	private Integer _peIdAttributeIndex = null;		

	/**
	 * Index of the operatorName attribute.
	 */
	private Integer _operatorNameAttributeIndex = null;		
	
	/**
	 * Index of the eventTimestamp attribute.
	 */
	private Integer _eventTimestampAttributeIndex = null;	

	/**
	 * Index of the notifyType attribute.
	 */
	private Integer _notifyTypeAttributeIndex = null;	
	
	/**
	 * Index of the message attribute.
	 */
	private Integer _messageAttributeIndex = null;

	/**
	 * Index of the sequence attribute.
	 */
	private Integer _sequenceAttributeIndex = null;
	
	/**
	 * Index of the messagesSkipped attribute.
	 */
	private Integer _messagesSkippedAttributeIndex = null;

	
	/**
	 * Determine the indexes of output attributes and verify their types.
	 * 
	 * @param port
	 */
	public LogTupleContainer(OperatorContext context, StreamingOutput<OutputTuple> port) {
		_context = context;
		// Create a tuple once.
		_port = port;
		
		StreamSchema schema = port.getStreamSchema();
		// Domain-related attributes.
		if (_domainIdAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("domainId");
			_domainIdAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1);
		}
		// Instance-related attributes.
		if (_instanceIdAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("instanceId");
			_instanceIdAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1);
		}
		// Job-related attributes.
		if (_jobIdAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("jobId");
			_jobIdAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.INT64 ? attribute.getIndex() : -1);
		}
		// PE-related attributes.
		if (_resourceAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("resource");
			_resourceAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1);
		}
		if (_peIdAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("peId");
			_peIdAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.INT64 ? attribute.getIndex() : -1);
		}
		if (_operatorNameAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("operatorName");
			_operatorNameAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1);
		}
		if (_eventTimestampAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("eventTimestamp");
			_eventTimestampAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.TIMESTAMP ? attribute.getIndex() : -1);
		}
		if (_notifyTypeAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("notifyType");
			_notifyTypeAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1);
		}
		if (_messageAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("message");
			_messageAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1);
		}
		if (_sequenceAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("sequence");
			_sequenceAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.INT64 ? attribute.getIndex() : -1);
		}
		if (_messagesSkippedAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("messagesSkipped");
			_messagesSkippedAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.BOOLEAN ? attribute.getIndex() : -1);
		}
		
	}
	
	
	public void punctuate(Punctuation mark) throws Exception {
		_port.punctuate(mark);
	}
	
	/**
	 * Convert the notification to a tuple, using {@link #getAttributes(Notification, Object)}.
	 * @param notification Notification object.
	 * @param handback Handback object.
	 * @return Tuple to be submitted.
	 */
	public Tuple getTuple(
			final Notification notification,
			final String domainId,
			final String instance,
			final String resource,
			final BigInteger pe,
			final BigInteger job,
			final String operator,
			final String text,
			final boolean messagesSkipped) {
		return _port.getStreamSchema().getTuple(getAttributes(
				notification,
				domainId,
				instance,
				resource,
				pe,
				job,
				operator,
				text,
				messagesSkipped
				));
	}
	
	/**
	 * Convert the notification to a {@code Map} of attributes keyed by attribute name.
	 * @param notification Notification object.
	 * @param handback Handback object.
	 * @return Map containing attributes
	 */
	protected Map<String, Object> getAttributes(
			final Notification notification,
			final String domainId,
			final String instanceId,
			final String resource,
			final BigInteger peId,
			final BigInteger jobId,
			final String operator,
			final String text,
			final boolean messagesSkipped
			) {
		
		final Map<String, Object> attributes = new HashMap<String, Object>();
		
		if (_domainIdAttributeIndex != -1) {
			if (domainId != null) {
				attributes.put("domainId", new RString(domainId));
			}
		}
		if (_instanceIdAttributeIndex != -1) {
			if (instanceId != null) {
				attributes.put("instanceId", new RString(instanceId));
			}
		}
		if (_jobIdAttributeIndex != -1) {
			if (jobId != null) {
				attributes.put("jobId", jobId.longValue());
			}
		}
		if (_resourceAttributeIndex != -1) {
			if (resource != null) {
				attributes.put("resource", new RString(resource));
			}
		}
		if (_peIdAttributeIndex != -1) {
			if (peId != null) {
				attributes.put("peId", peId.longValue());
			}
		}
		if (_operatorNameAttributeIndex != -1) {
			if (operator != null) {
				attributes.put("operatorName", new RString(operator));
			}
		}
		if (_eventTimestampAttributeIndex != -1) {
			attributes.put("eventTimestamp", Timestamp.getTimestamp(notification.getTimeStamp()));
		}
		if (_notifyTypeAttributeIndex != -1) {
			attributes.put("notifyType", new RString(notification.getType()));
		}		
		if (_sequenceAttributeIndex != -1) {
			attributes.put("sequence", notification.getSequenceNumber());
		}

		if (_messageAttributeIndex != -1) {
			if (notification.getMessage() != null) {
				attributes.put("message", new RString(notification.getMessage()));
			}
			if (null != text) {
				attributes.put("message", new RString(text));
			}
		}
		if (_messagesSkippedAttributeIndex != -1) {
			attributes.put("messagesSkipped", messagesSkipped);
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
