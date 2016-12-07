package com.ibm.streamsx.metrics;


import java.net.InetAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.PrimitiveOperator;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

@PrimitiveOperator(name="MetricsSink", namespace="com.ibm.streamsx.metrics",
description="Java Operator MetricsSink")
@InputPorts({@InputPortSet(description="Port that ingests tuples", cardinality=1, optional=false, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious), @InputPortSet(description="Optional input ports", optional=true, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@Libraries({
	// As described here:
	// http://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.0/com.ibm.streams.dev.doc/doc/jmxapi-start.html
	// Environment variables (@...@) are evaluated during compile-time and must
	// be identical in the run-time environment.
	"@STREAMS_INSTALL@/lib/com.ibm.streams.management.jmxmp.jar",
	"@STREAMS_INSTALL@/lib/com.ibm.streams.management.mx.jar",
	"@STREAMS_INSTALL@/ext/lib/jmxremote_optional.jar",
	"opt/downloaded/*"
	})
public class MetricsSink extends AbstractOperator {
	
	// InfluxDB variables.
	InfluxDB influxDB = null;
	String dbName = null;
	BatchPoints batchPoints = null;
	String jobName = null;
	
	// ElasticSearch variables.
	TransportClient client = null;
	XContentBuilder builder = null;
	IndexResponse response = null;
	
	@SuppressWarnings("resource")
	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
		super.initialize(context);
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
        
        // Connect to InfluxDB server.
        influxDB = InfluxDBFactory.connect("http://localhost:8086", "admin", "admin");
    	dbName = "streamsDb";
    	influxDB.createDatabase(dbName);
        
    	// Initialize object to contain batch of points.
    	batchPoints = BatchPoints
	    	.database(dbName)
    		.tag("async", "true")
            .retentionPolicy("autogen")
            .consistency(ConsistencyLevel.ALL)
            .build();
    	
    	// Connect to ElasticSearch server.
		client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
	}

    @Override
    public synchronized void allPortsReady() throws Exception {
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " all ports are ready in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
    }

    @Override
    public void process(StreamingInput<Tuple> stream, Tuple tuple)
            throws Exception {

    	// Create InfluxDB point to output.
        Point point = Point.measurement("metrics")
            .time(tuple.getLong("lastTimeRetrieved"), TimeUnit.MILLISECONDS)
            .addField("domainName", tuple.getString("domainName"))
            .addField("instanceName", tuple.getString("instanceName"))
            .addField("jobId", tuple.getString("jobId"))
            .addField("jobName", tuple.getString("jobName"))
            .addField("operatorName", tuple.getString("operatorName"))
            .addField("portIndex", tuple.getInt("portIndex"))
            .addField(tuple.getString("metricName"), tuple.getLong("metricValue"))
            .build();
        
        // Store in batch until window marker is received.
        batchPoints.point(point);
        
        // Create ElasticSearch JSON to output.
        DateFormat df = new SimpleDateFormat("yyyy'-'MM'-'dd'T'HH':'mm':'ss.SSSZZ");
        
        builder = XContentFactory.jsonBuilder()
        		.startObject()
	        		.field("lastTimeRetrieved", df.format(new Date((tuple.getLong("lastTimeRetrieved")))))
        			.field("domainName", tuple.getString("domainName"))
        			.field("instanceName", tuple.getString("instanceName"))
        			.field("jobId", tuple.getString("jobId"))
        			.field("jobName", tuple.getString("jobName"))
        			.field("operatorName", tuple.getString("operatorName"))
        			.field("portIndex", tuple.getString("portIndex"))
    				.field(tuple.getString("metricName"), 
    						Integer.parseInt(tuple.getString("metricValue")))
        		.endObject();
    }
    
    @Override
    public void processPunctuation(StreamingInput<Tuple> stream,
    		Punctuation mark) throws Exception {

    	// Output metrics to InfluxDB.
    	if(batchPoints != null) {
    		influxDB.write(batchPoints);
    	}
        
        // Output metrics to ElasticSearch.
        if(builder != null) {
	        response = client.prepareIndex("streamsdb", "metrics")
	            	.setSource(builder)
	            	.get();
        }
    }

    @Override
    public synchronized void shutdown() throws Exception {
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );

        // Close connection to ElasticSearch server.
        client.close();
        
        super.shutdown();
    }
    
}
