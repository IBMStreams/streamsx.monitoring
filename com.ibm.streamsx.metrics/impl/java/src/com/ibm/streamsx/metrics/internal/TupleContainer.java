package com.ibm.streamsx.metrics.internal;

import java.math.BigInteger;

import com.ibm.streams.operator.Attribute;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Type;

public class TupleContainer {

	/**
	 * Index of the domainName attribute.
	 */
	private Integer _domainNameAttributeIndex = null;
	
	/**
	 * Index of the instanceName attribute.
	 */
	private Integer _instanceNameAttributeIndex = null;

	/**
	 * Index of the jobId attribute.
	 */
	private Integer _jobIdAttributeIndex = null;

	/**
	 * Index of the jobName attribute.
	 */
	private Integer _jobNameAttributeIndex = null;

	/**
	 * Index of the operatorName attribute.
	 */
	private Integer _operatorNameAttributeIndex = null;

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
	public TupleContainer(StreamingOutput<OutputTuple> port) {
		// Create a tuple once.
		_port = port;
		_tuple = port.newTuple();
		StreamSchema schema = port.getStreamSchema();
		// Domain-related attributes.
		if (_domainNameAttributeIndex == null) {
			Attribute attribute = schema.getAttribute("domainName");
			_domainNameAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1) ;
		}
		// Instance-related attributes.
		if (_instanceNameAttributeIndex  == null) {
			Attribute attribute = schema.getAttribute("instanceName");
			_instanceNameAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1) ;
		}
		// Job-related attributes.
		if (_jobIdAttributeIndex  == null) {
			Attribute attribute = schema.getAttribute("jobId");
			_jobIdAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.INT64 ? attribute.getIndex() : -1) ;
			
		}
		if (_jobNameAttributeIndex  == null) {
			Attribute attribute = schema.getAttribute("jobName");
			_jobNameAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1) ;
		}
		// Operator-related attributes.
		if (_operatorNameAttributeIndex  == null) {
			Attribute attribute = schema.getAttribute("operatorName");
			_operatorNameAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1) ;
		}
		// Metric-related attributes.
		if (_metricNameAttributeIndex  == null) {
			Attribute attribute = schema.getAttribute("metricName");
			_metricNameAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.RSTRING ? attribute.getIndex() : -1) ;
		}
		if (_metricValueAttributeIndex  == null) {
			Attribute attribute = schema.getAttribute("metricValue");
			_metricValueAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.INT64 ? attribute.getIndex() : -1) ;
		}
		if (_lastTimeRetrievedAttributeIndex  == null) {
			Attribute attribute = schema.getAttribute("lastTimeRetrieved");
			_lastTimeRetrievedAttributeIndex = Integer.valueOf(attribute != null && attribute.getType().getMetaType() == Type.MetaType.INT64 ? attribute.getIndex() : -1) ;
		}
	}
		
	/**
	 * Optionally set the domain name in the output tuple.
	 * 
	 * @param domainName
	 */
	public void setDomainName(String domainName) {
		if (_domainNameAttributeIndex != -1) {
			_tuple.setString(_domainNameAttributeIndex, domainName);
		}
	}

	/**
	 * Optionally set the instance name in the output tuple.
	 * 
	 * @param domainName
	 */
	public void setInstanceName(String instanceName) {
		if (_instanceNameAttributeIndex != -1) {
			_tuple.setString(_instanceNameAttributeIndex, instanceName);
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
