//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.notification;

import java.math.BigInteger;
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

public class TupleContainer {

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
	 * Index of the jobName attribute.
	 */
	private Integer _jobNameAttributeIndex = null;

	/**
	 * Index of the resource attribute.
	 */
	private Integer _resourceAttributeIndex = null;

	/**
	 * Index of the peId attribute.
	 */
	private Integer _peIdAttributeIndex = null;	

	/**
	 * Index of the peHealth attribute.
	 */
	private Integer _peHealthAttributeIndex = null;	

	/**
	 * Index of the peStatus attribute.
	 */
	private Integer _peStatusAttributeIndex = null;

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
	 * Index of the userData attribute.
	 */
	private Integer _userDataAttributeIndex = null;
	
	/**
	 * Index of the message attribute.
	 */
	private Integer _messageAttributeIndex = null;

	/**
	 * Index of the sequence attribute.
	 */
	private Integer _sequenceAttributeIndex = null;

	/**
	 * Index of the _handback attribute.
	 */
	private Integer _handbackAttributeIndex = null;	
	
	
	/**
	 * Determine the indexes of output attributes and verify their types.
	 * 
	 * @param port
	 */
	public TupleContainer(OperatorContext context, StreamingOutput<OutputTuple> port) {
		_context = context;
		// Create a tuple once.
		_port = port;
		
		StreamSchema schema = port.getStreamSchema();
		// Domain-related attributes.
		if (_domainIdAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("domainId");
			_domainIdAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1) ;
		}
		// Instance-related attributes.
		if (_instanceIdAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("instanceId");
			_instanceIdAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1) ;
		}
		// Job-related attributes.
		if (_jobIdAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("jobId");
			_jobIdAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.INT64 ? attribute.getIndex() : -1) ;
			
		}
		if (_jobNameAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("jobName");
			_jobNameAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1) ;
		}
		// PE-related attributes.
		if (_resourceAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("resource");
			_resourceAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1) ;
		}
		if (_peIdAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("peId");
			_peIdAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.INT64 ? attribute.getIndex() : -1) ;
		}
		if (_peHealthAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("peHealth");
			_peHealthAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1) ;
		}
		if (_peStatusAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("peStatus");
			_peStatusAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1) ;
		}
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
		if (_userDataAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("userData");
			_userDataAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1) ;
		}		
		if (_messageAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("message");
			_messageAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1) ;
		}
		if (_sequenceAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("sequence");
			_sequenceAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.INT64 ? attribute.getIndex() : -1) ;
		}		
		if (_handbackAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("handback");
			_handbackAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1) ;
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
			final Object handback,
			final String domainId,
			final String instanceId,
			final BigInteger jobId,
			final String jobName,
			final String resource,
			final BigInteger peId,
			final Object peHealth,
			final Object peStatus) {
		return _port.getStreamSchema().getTuple(getAttributes(
				notification,
				handback,
				domainId,
				instanceId,
				jobId,
				jobName,
				resource,
				peId,
				peHealth,
				peStatus));
	}
	
	/**
	 * Convert the notification to a {@code Map} of attributes keyed by attribute name.
	 * @param notification Notification object.
	 * @param handback Handback object.
	 * @return Map containing attributes
	 */
	protected Map<String, Object> getAttributes(
			final Notification notification,
			final Object handback,
			final String domainId,
			final String instanceId,
			final BigInteger jobId,
			final String jobName,
			final String resource,
			final BigInteger peId,
			final Object peHealth,
			final Object peStatus) {
		
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
		if (_jobNameAttributeIndex != -1) {
			if (jobName != null) {
				attributes.put("jobName", new RString(jobName));
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
		if (_peHealthAttributeIndex != -1) {
			if (peHealth != null) {
				attributes.put("peHealth", new RString(peHealth.toString()));
			}
		}
		if (_peStatusAttributeIndex != -1) {
			if (peStatus != null) {
				attributes.put("peStatus", new RString(peStatus.toString()));
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
		
		if (_userDataAttributeIndex != -1) {
			final Object userData = notification.getUserData();
			if (userData != null) {
				attributes.put("userData", new RString(userData.toString()));
			}
		}
		if (_handbackAttributeIndex != -1) {
			attributes.put("handback", handback.toString());
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
