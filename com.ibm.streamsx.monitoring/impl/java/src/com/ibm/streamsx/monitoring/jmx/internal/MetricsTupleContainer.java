//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.jmx.internal;

import java.math.BigInteger;

import com.ibm.streams.management.MetricMetadata.Kind;
import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Type;

public class MetricsTupleContainer {

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
	 * Index of the operatorName attribute.
	 */
	private Integer _operatorNameAttributeIndex = null;

	/**
	 * Index of the origin attribute.
	 */
	private Integer _originAttributeIndex = null;
	
	/**
	 * Index of the portIndex attribute.
	 */
	private Integer _portIndexAttributeIndex = null;
	
	/**
	 * Index of the connectionId attribute.
	 */
	private Integer _connectionIdAttributeIndex = null;
	
	/**
	 * Index of the channel attribute.
	 */
	private Integer _channelAttributeIndex = null;
	
	/**
	 * Index of the peId attribute.
	 */
	private Integer _peIdAttributeIndex = null;

	/**
	 * Index of the metricType attribute.
	 */
	private Integer _metricTypeAttributeIndex = null;

	/**
	 * Index of the metricKind attribute.
	 */
	private Integer _metricKindAttributeIndex = null;

	/**
	 * Index of the metricName attribute.
	 */
	private Integer _metricNameAttributeIndex = null;

	/**
	 * Index of the metricValue attribute.
	 */
	private Integer _metricValueAttributeIndex = null;

	/**
	 * Index of the lastTimeRetrieved attribute.
	 */
	private Integer _lastTimeRetrievedAttributeIndex = null;
	
	/**
	 * The output port.
	 */
	private StreamingOutput<OutputTuple> _port = null;
	
	/**
	 * The output tuple.
	 */
	private OutputTuple _tuple = null;

	/**
	 * Determine the indexes of output attributes and verify their types.
	 * 
	 * @param port
	 */
	public MetricsTupleContainer(StreamingOutput<OutputTuple> port) {
		// Create a tuple once.
		_port = port;
		_tuple = port.newTuple();
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
		// Operator-related attributes.
		if (_operatorNameAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("operatorName");
			_operatorNameAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1) ;
		}
		// Operator- and PE-related attributes.
		if (_resourceAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("resource");
			_resourceAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1) ;
		}
		if (_channelAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("channel");
			_channelAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.INT32 ? attribute.getIndex() : -1) ;
		}
		if (_peIdAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("peId");
			_peIdAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.INT64 ? attribute.getIndex() : -1) ;
		}
		// Port-related attributes.
		if (_portIndexAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("portIndex");
			_portIndexAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.INT32 ? attribute.getIndex() : -1) ;
		}
		// Connection-related attributes.
		if (_connectionIdAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("connectionId");
			_connectionIdAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1) ;
		}
		// Metric-related attributes.
		if (_originAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("origin");
			_originAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.ENUM ? attribute.getIndex() : -1) ;
		}
		if (_metricTypeAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("metricType");
			_metricTypeAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1) ;
		}
		if (_metricKindAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("metricKind");
			_metricKindAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1) ;
		}
		if (_metricNameAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("metricName");
			_metricNameAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1) ;
		}
		if (_metricValueAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("metricValue");
			_metricValueAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.INT64 ? attribute.getIndex() : -1) ;
		}
		if (_lastTimeRetrievedAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("lastTimeRetrieved");
			_lastTimeRetrievedAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.INT64 ? attribute.getIndex() : -1) ;
		}
	}
		
	/**
	 * Optionally set the domain id in the output tuple.
	 * 
	 * @param domainId
	 */
	public void setDomainId(String domainId) {
		if (_domainIdAttributeIndex != -1) {
			_tuple.setString(_domainIdAttributeIndex, domainId);
		}
	}

	/**
	 * Optionally set the instance id in the output tuple.
	 * 
	 * @param instanceId
	 */
	public void setInstanceId(String instanceId) {
		if (_instanceIdAttributeIndex != -1) {
			_tuple.setString(_instanceIdAttributeIndex, instanceId);
		}
	}
	
	/**
	 * Optionally set the job id in the output tuple.
	 * 
	 * @param jobId
	 */
	public void setJobId(BigInteger jobId) {
		if (_jobIdAttributeIndex != -1) {
			_tuple.setLong(_jobIdAttributeIndex, jobId.longValue());
		}
	}

	/**
	 * Optionally set the job name in the output tuple.
	 * 
	 * @param jobName
	 */
	public void setJobName(String jobName) {
		if (_jobNameAttributeIndex != -1) {
			_tuple.setString(_jobNameAttributeIndex, jobName);
		}
	}

	/**
	 * Optionally set the resource, which is the host, in the output tuple.
	 * 
	 * @param resource
	 */
	public void setResource(String resource) {
		if (_resourceAttributeIndex != -1) {
			_tuple.setString(_resourceAttributeIndex, resource);
		}
	}

	/**
	 * Optionally set the operator name in the output tuple.
	 * 
	 * @param operatorName
	 */
	public void setOperatorName(String operatorName) {
		if (_operatorNameAttributeIndex != -1) {
			_tuple.setString(_operatorNameAttributeIndex, operatorName);
		}
	}

	/**
	 * Optionally set the port index in the output tuple.
	 * 
	 * @param portIndex
	 */
	public void setPortIndex(int portIndex) {
		if (_portIndexAttributeIndex != -1) {
			_tuple.setInt(_portIndexAttributeIndex, portIndex);
		}
	}
	
	/**
	 * Optionally set the connection id in the output tuple.
	 * 
	 * @param connectionId
	 */
	public void setConnectionId(String connectionId) {
		if (_connectionIdAttributeIndex != -1) {
			_tuple.setString(_connectionIdAttributeIndex, connectionId);
		}
	}

	/**
	 * Optionally set the origin in the output tuple.
	 * 
	 * @param origin
	 */
	public void setOrigin(String origin) {
		if (_originAttributeIndex != -1) {
			_tuple.setString(_originAttributeIndex, origin);
		}
	}

	/**
	 * Optionally set the channel in the output tuple. If the channel information
	 * is not available, set -1.
	 * 
	 * @param channel
	 */
	public void setChannel(Integer channel) {
		if (_channelAttributeIndex != -1) {
			_tuple.setInt(_channelAttributeIndex, (channel == null ? -1 : channel));
		}
	}

	/**
	 * Optionally set the PE id in the output tuple.
	 * 
	 * @param peId
	 */
	public void setPeId(BigInteger peId) {
		if (_peIdAttributeIndex != -1) {
			_tuple.setLong(_peIdAttributeIndex, peId.longValue());
		}
	}

	/**
	 * Optionally set the metric type in the output tuple.
	 * 
	 * @param metricType
	 */
	public void setMetricType(com.ibm.streams.management.MetricMetadata.Type metricType) {
		if (_metricTypeAttributeIndex != -1) {
			_tuple.setString(_metricTypeAttributeIndex, metricType.toString());
		}
	}

	/**
	 * Optionally set the metric kind in the output tuple.
	 * 
	 * @param metricKind
	 */
	public void setMetricKind(Kind metricKind) {
		if (_metricKindAttributeIndex != -1) {
			_tuple.setString(_metricKindAttributeIndex, metricKind.toString());
		}
	}

	/**
	 * Optionally set the metric name in the output tuple.
	 * 
	 * @param metricName
	 */
	public void setMetricName(String metricName) {
		if (_metricNameAttributeIndex != -1) {
			_tuple.setString(_metricNameAttributeIndex, metricName);
		}
	}

	/**
	 * Optionally set the metric value in the output tuple.
	 * 
	 * @param metricValue
	 */
	public void setMetricValue(long metricValue) {
		if (_metricValueAttributeIndex != -1) {
			_tuple.setLong(_metricValueAttributeIndex, metricValue);
		}
	}

	/**
	 * Optionally set the lastTimeRetrieved in the output tuple.
	 * 
	 * @param lastTimeRetrieved
	 */
	public void setLastTimeRetrieved(long lastTimeRetrieved) {
		if (_lastTimeRetrievedAttributeIndex != -1) {
			_tuple.setLong(_lastTimeRetrievedAttributeIndex, lastTimeRetrieved);
		}
	}

	public void submit() throws Exception {
		// Submit tuple to output stream.            
		_port.submit(_tuple);	
	}

	public void punctuate(Punctuation mark) throws Exception {
		_port.punctuate(mark);
	}

}
