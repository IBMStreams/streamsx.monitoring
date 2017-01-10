package com.ibm.streamsx.metrics;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

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
import com.ibm.streams.operator.model.Parameter;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

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
		name="MetricsInfluxDBSink",
		namespace="com.ibm.streamsx.metrics",
		description=MetricsInfluxDBSink.DESC_OPERATOR
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
public class MetricsInfluxDBSink extends AbstractOperator {

	// ------------------------------------------------------------------------
	// Documentation.
	// Attention: To add a newline, use \\n instead of \n.
	// ------------------------------------------------------------------------

	static final String DESC_OPERATOR = 
			"The MetricsSink operator received metrics as tuples from the "
			+ "MetricsSource operator and outputs these metrics to "
			+ "ElasticSearch and, optionally, InfluxDB.\\n"
			+ "\\n"
			+ "By default, the MetricsSink does not output to InfluxDB. In "
			+ "order to enable this feature, set the optional parameter, "
			+ "outputToInfluxDB to true.\\n"
			+ "\\n"
			+ "The MetricsSink uses the default hostname and hostport that "
			+ "comes configured with ElasticSearch and InfluxDB. If you have "
			+ "custom settings, this can be configured by editing the optional "
			+ "hostname, hostport, username, and password parameters.\\n"
			+ "\\n"
			+ "Per default, the name of the database to output to is "
			+ "\"streamsdb\". This can be changed by editing to optional "
			+ "parameter, databaseName.\\n"
			+ "\\n"
			+ "Once the data is outputted to either ElasticSearch or InfluxDB, "
			+ "you can query the database and create custom graphs to display "
			+ "this data with graphing tools such as Grafana and Kibana.\\n"
			;

	private static final String DESC_INFLUXDB_USERNAME = 
			"Specifies the username of the InfluxDB server.";

	private static final String DESC_INFLUXDB_PASSWORD = 
			"Specifies the password of the InfluxDB server.";

	private static final String DESC_INFLUXDB_HOSTNAME = 
			"Specifies the hostname of the InfluxDB server.";

	private static final String DESC_INFLUXDB_HOSTPORT = 
			"Specifies the hostport of the InfluxDB server.";
	
	private static final String DESC_DATABASE_NAME = 
			"Specifies the name for the databases.";

	@Parameter(
			optional=false,
			description=MetricsInfluxDBSink.DESC_INFLUXDB_USERNAME
			)
	public void set_influxdb_username(String username) {
		influxUsername = username;
	}

	@Parameter(
			optional=false,
			description=MetricsInfluxDBSink.DESC_INFLUXDB_PASSWORD
			)
	public void set_influxdb_password(String password) {
		influxPassword = password;
	}

	@Parameter(
			optional=false,
			description=MetricsInfluxDBSink.DESC_INFLUXDB_HOSTNAME
			)
	public void set_influxdb_hostname(String hostname) {
		influxHostName = hostname;
	}

	@Parameter(
			optional=false,
			description=MetricsInfluxDBSink.DESC_INFLUXDB_HOSTPORT
			)
	public void set_influxdb_hostport(int hostport) {
		influxHostPort = String.valueOf(hostport);
	}
	
	@Parameter(
			optional=false,
			description=MetricsInfluxDBSink.DESC_DATABASE_NAME
			)
	public void set_database_name(String name) {
		databaseName = name;
	}

	
	// ------------------------------------------------------------------------
	// Implementation.
	// ------------------------------------------------------------------------
	
	/**
	 * InfluxDB configuration.
	 */
	private InfluxDB influxDB = null;
	private String influxUsername = "admin";
	private String influxPassword = "admin";
	private String influxHostName = "http://localhost";
	private String influxHostPort = "8086";
	private BatchPoints batchPoints = null;

	/**
	 * Database name.
	 */
	private String databaseName = "streamsdb";
	
	/**
	 * Logger for tracing.
	 */
	@SuppressWarnings("unused")
	private static Logger _trace = Logger.getLogger(MetricsInfluxDBSink.class.getName());
	
	/**
     * Initialize this operator. Called once before any tuples are processed.
     * @param context OperatorContext for this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
		super.initialize(context);
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
        
        // Connect to InfluxDB server.
        influxDB = InfluxDBFactory.connect(influxHostName + ":" + influxHostPort, influxUsername, influxPassword);
    	influxDB.createDatabase(databaseName);

    	// Initialize object to contain batch of points.
    	batchPoints = BatchPoints
	    	.database(databaseName)
    		.tag("async", "true")
            .retentionPolicy("autogen")
            .consistency(ConsistencyLevel.ALL)
            .build();
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

    	// Output metrics to InfluxDB.
    	if(batchPoints != null) {
    		influxDB.write(batchPoints);
    	}
    }

    /**
     * Shutdown this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void shutdown() throws Exception {
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );

        // Must call super.shutdown()
        super.shutdown();
    }
    
}
