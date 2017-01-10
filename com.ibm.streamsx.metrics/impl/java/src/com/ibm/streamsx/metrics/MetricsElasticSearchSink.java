package com.ibm.streamsx.metrics;


import java.net.InetAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;

import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * Class for an operator that consumes tuples. 
 * The following event methods from the Operator interface can be called:
 * </p>
 * <ul>
 * <li><code>initialize()</code> to perform operator initialization</li>
 * <li>allPortsReady() notification indicates the operator's ports are ready to process and submit tuples</li> 
 * <li>process() handles a tuple arriving on an input port 
 * <li>processPuncuation() handles a punctuation mark arriving on an input port 
 * <li>shutdown() to shutdown the operator. A shutdown request may occur at any time, 
 * such as a request to stop a PE or cancel a job. 
 * Thus the shutdown() may occur while the operator is processing tuples, punctuation marks, 
 * or even during port ready notification.</li>
 * </ul>
 * <p>With the exception of operator initialization, all the other events may occur concurrently with each other, 
 * which lead to these methods being called concurrently by different threads.</p> 
 */
@PrimitiveOperator(
		name="MetricsElasticSearchSink",
		namespace="com.ibm.streamsx.metrics",
		description=MetricsElasticSearchSink.DESC_OPERATOR
		)
@InputPorts({
	@InputPortSet(
			description="Port that ingests tuples",
			cardinality=1,
			optional=false,
			windowingMode=WindowMode.NonWindowed,
			windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious
			)
})
@Libraries({
	"opt/downloaded/*"
	})
public class MetricsElasticSearchSink extends AbstractOperator {

	// ------------------------------------------------------------------------
	// Documentation.
	// Attention: To add a newline, use \\n instead of \n.
	// ------------------------------------------------------------------------

	static final String DESC_OPERATOR = 
			"The MetricsElasticSearchDBSink operator receives metrics as tuples from "
			+ "the MetricsSource operator and outputs these metrics to an ElasticSearch "
			+ "database.\\n"
			+ "\\n"
			+ "The MetricsElasticSearchSink requires a hostname, and hostport to be "
			+ "specified in its parameters.\\n"
			+ "\\n"
			+ "By default, the hostname is 'localhost', and the hostport "
			+ "is 9300.\\n"
			+ "\\n"
			+ "An indexName and type must also be specified. The index is the "
			+ "highest level element. It can be thought of as the database. The "
			+ "index can hold various types. Types can be thought of as tables.\\n"
			+ "\\n"
			+ "A timestampName must be specified for time-based queries like monitoring "
			+ "how a tuple attribute's value changes over time.\\n"
			+ "\\n"
			+ "Once the data is outputted to ElasticSearch, the user can query the "
			+ "database and create custom graphs to display this data with graphing "
			+ "tools such as Grafana and Kibana. An example query would be:\\n"
			+ "`SELECT 'metricValue' FROM '<databaseName>' WHERE 'metricName'="
			+ "'<metricName>'`"
			+ "\\n"
			;

	private static final String DESC_ELASTICSEARCH_HOSTNAME = 
			"Specifies the hostname of the ElasticSearch server.";

	private static final String DESC_ELASTICSEARCH_HOSTPORT = 
			"Specifies the hostport of the ElasticSearch server.";
	
	private static final String DESC_INDEX_NAME = 
			"Specifies the name for the index.";
	
	private static final String DESC_TYPE_NAME = 
			"Specifies the name for the type.";
	
	private static final String DESC_TIMESTAMP_NAME = 
			"Specifies the name for the timestamp attribute.";
	
	@Parameter(
			optional=false,
			description=MetricsElasticSearchSink.DESC_ELASTICSEARCH_HOSTNAME
			)
	public void set_elasticsearch_hostname(String hostname) {
		elasticSearchHostName = hostname;
	}

	@Parameter(
			optional=false,
			description=MetricsElasticSearchSink.DESC_ELASTICSEARCH_HOSTPORT
			)
	public void set_elasticsearch_hostport(int hostport) {
		elasticSearchHostPort = hostport;
	}
	
	@Parameter(
			optional=false,
			description=MetricsElasticSearchSink.DESC_INDEX_NAME
			)
	public void set_index_name(String name) {
		indexName = name;
	}
	
	@Parameter(
			optional=false,
			description=MetricsElasticSearchSink.DESC_TYPE_NAME
			)
	public void set_type_name(String name) {
		typeName = name;
	}
	
	@Parameter(
			optional=false,
			description=MetricsElasticSearchSink.DESC_TIMESTAMP_NAME
			)
	public void set_timestamp_name(String name) {
		timestampName = name;
	}

	
	// ------------------------------------------------------------------------
	// Implementation.
	// ------------------------------------------------------------------------

	/**
	 * ElasticSearch configuration.
	 */
	private TransportClient client = null;
	private XContentBuilder builder = null;
	private String elasticSearchHostName = "localhost";
	private int elasticSearchHostPort = 9300;

	/**
	 * Index name.
	 */
	private String indexName = null;
	
	/**
	 * Type name.
	 */
	private String typeName = null;
	
	/**
	 * Timestamp name.
	 */
	private String timestampName = null;
	
	/**
	 * Logger for tracing.
	 */
	@SuppressWarnings("unused")
	private static Logger _trace = Logger.getLogger(MetricsElasticSearchSink.class.getName());
	
	/**
     * Initialize this operator. Called once before any tuples are processed.
     * @param context OperatorContext for this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
	@SuppressWarnings("resource")
	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
		super.initialize(context);
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );

    	// Connect to ElasticSearch server.
        Settings settings = Settings.builder().put("cluster.name","elasticsearch").build();
		client = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(elasticSearchHostName), elasticSearchHostPort));
		
		// Create index if it doesn't exist.
		if(!client.admin().indices().prepareExists(indexName).execute().actionGet().isExists()) {
			client.admin().indices().create(Requests.createIndexRequest(indexName)).actionGet();
		}
	}

	/**
     * Notification that initialization is complete and all input and output ports 
     * are connected and ready to receive and submit tuples.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void allPortsReady() throws Exception {
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " all ports are ready in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
    }

	/**
     * Process an incoming tuple that arrived on the specified port.
     * @param stream Port the tuple is arriving on.
     * @param tuple Object representing the incoming tuple.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public void process(StreamingInput<Tuple> stream, Tuple tuple)
            throws Exception {
        
    	// Collect fields to add to JSON output.
		StreamSchema schema = tuple.getStreamSchema();
    	Set<String> attributeNames = schema.getAttributeNames();
    	Map<String, Object> fieldsToAdd = new HashMap<String, Object>();
    	
    	for(String attributeName : attributeNames) {
    		fieldsToAdd.put(attributeName, tuple.getObject(attributeName));
//        	_trace.error("Attribute Name:" + attributeName + "; Attribute Value:" + tuple.getObject(attributeName));
    	}
        
    	// Add timestamp for time-based queries.
        DateFormat df = new SimpleDateFormat("yyyy'-'MM'-'dd'T'HH':'mm':'ss.SSSZZ");
    	fieldsToAdd.put(timestampName, df.format(new Date(System.currentTimeMillis())));

        // Create ElasticSearch JSON to output.
        builder = XContentFactory.jsonBuilder().map(fieldsToAdd);
        
        // Output metrics to ElasticSearch.
        if(builder != null) {
	        client.prepareIndex(indexName, typeName)
	            	.setSource(builder)
	            	.get();
        }
    }
    
	/**
     * Process an incoming punctuation that arrived on the specified port.
     * @param stream Port the punctuation is arriving on.
     * @param mark The punctuation mark
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public void processPunctuation(StreamingInput<Tuple> stream,
    		Punctuation mark) throws Exception {
    }

	/**
     * Shutdown this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void shutdown() throws Exception {
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );

        // Close connection to ElasticSearch server.
        client.close();
        
        super.shutdown();
    }
    
}
