//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.metrics;

import java.net.MalformedURLException;
import java.util.HashMap;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streamsx.metrics.internal.OperatorConfiguration;
import com.ibm.streamsx.metrics.internal.TupleContainer;
import com.ibm.streamsx.metrics.internal.filter.Filters;
import com.ibm.streamsx.metrics.internal.DomainHandler;
import com.ibm.streamsx.metrics.internal.EmitMetricTupleMode;
import com.ibm.streams.operator.model.Parameter;

/**
 * A source operator that does not receive any input streams and produces new tuples. 
 * The method <code>produceTuples</code> is called to begin submitting tuples.
 * <P>
 * For a source operator, the following event methods from the Operator interface can be called:
 * </p>
 * <ul>
 * <li><code>initialize()</code> to perform operator initialization</li>
 * <li>allPortsReady() notification indicates the operator's ports are ready to process and submit tuples</li> 
 * <li>shutdown() to shutdown the operator. A shutdown request may occur at any time, 
 * such as a request to stop a PE or cancel a job. 
 * Thus the shutdown() may occur while the operator is processing tuples, punctuation marks, 
 * or even during port ready notification.</li>
 * </ul>
 * <p>With the exception of operator initialization, all the other events may occur concurrently with each other, 
 * which lead to these methods being called concurrently by different threads.</p> 
 * 
 * Do not use the @Libraries annotation because the operator requires class
 * libraries from the IBM Streams' installation, and the STREAMS_INSTALL
 * environment variable would be evaluated during compile-time only. This
 * would require identical install locations for the build and run-time
 * environment.
 */
@PrimitiveOperator(
		name="MetricsSource",
		namespace="com.ibm.streamsx.metrics",
		description=MetricsSource.DESC_OPERATOR
		)
@OutputPorts({
	@OutputPortSet(
			cardinality=1,
			optional=false,
			windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating,
			description=MetricsSource.DESC_OUTPUT_PORT
			)
})
@Icons(
		location16 = "icons/MetricsSource_16.gif", 
		location32 = "icons/MetricsSource_32.gif"
)
public class MetricsSource extends AbstractOperator {

	// ------------------------------------------------------------------------
	// Documentation.
	// Attention: To add a newline, use \\n instead of \n.
	// ------------------------------------------------------------------------
	
	static final String DESC_OPERATOR = 
			"The MetricsSource operator uses the "
			+ "[http://www.ibm.com/support/knowledgecenter/SSCRJU_4.2.0/com.ibm.streams.ref.doc/doc/jmxapi.html|JMX] "
			+ "API to retrieve metrics from one or more jobs, and provides "
			+ "metric changes as tuple stream.\\n"
			+ "\\n"
			+ "As an application developer, you provide the so-called filter "
			+ "document. The filter document specifies patterns for domain, "
			+ "instance, job, operator, and metric names. It also specifies "
			+ "which name patterns are related, for example: For a domain X "
			+ "monitor all instances, whereas in each instance only jobs with "
			+ "a Y in their names shall be evaluated. For another domain Z, "
			+ "only jobs with a name ending with XYZ, are monitored, etc.\\n"
			+ "\\n"
			+ "If the MetricsSource evaluates whether, for example, a custom "
			+ "metric of an operator shall be retrieved periodically, the name "
			+ "patterns are applied. A custom metric is uniquely identified "
			+ "with the domain, instance, job, operator, and metric name. All "
			+ "these parts must match to the corresponding set of related name "
			+ "patterns.\\n"
			+ "\\n"
			+ "Per default, the MetricsSource operator monitors neither any "
			+ "domain, nor any instances, nor job, nor any other Streams job-"
			+ "related object. Only those objects (and their parents) that "
			+ "match the specified filters, are monitored.\\n"
			+ "\\n"
			+ "The MetricsSource operator monitors filter-matching domains, "
			+ "instances, and jobs that are running while the application that "
			+ "uses the MetricsSource operator, starts. Furthermore, the "
			+ "operator gets notifications for created and deleted instances, "
			+ "and submitted and cancelled jobs. Thererfore, the operator can "
			+ "retrieve metrics from filter-matching jobs that are submitted "
			+ "in the future.\\n"
			+ "\\n"
			+ "+ Filter document\\n"
			+ "\\n"
			+ "The filter document specifies patterns for domain, instance, "
			+ "job, operator, and metric names, and their relations.\\n"
			+ "\\n"
			+ "The filter document is a JSON-encoded text file that is "
			+ "configured with the **filterDocument** parameter.\\n"
			+ "\\n"
			;
	
	protected static final String DESC_OUTPUT_PORT = 
			"The MetricsSource operator emits a metric tuple to this "
			+ "output port for each metric, for which the operator "
			+ "identifies a changed value. You can use the "
			+ "[type:com.ibm.streamsx.metrics::Notification|Notification] "
			+ "tuple type, or any subset of the attributes specified for this "
			+ "type. After each scan cycle, the operator emits a WindowMarker "
			+ "to this port."
			;
	
	private static final String DESC_PARAM_CONNECTION_URL = 
			"Specifies the connection URL as returned by the `streamtool "
			+ "getjmxconnect` command.";
	
	private static final String DESC_PARAM_USER = 
			"Specifies the user that is required for the JMX connection.";
	
	private static final String DESC_PARAM_PASSWORD = 
			"Specifies the password that is required for the JMX connection.";
	
	private static final String DESC_PARAM_DOMAIN_ID = 
			"Specifies the domain id that is monitored. If no domain id is "
			+ "specified, the domain id under which this operator is running "
			+ "is used. If the operator is running in a standalone application "
			+ "it raises an exception and aborts.";
	
	private static final String DESC_PARAM_RETRY_PERIOD = 
			"Specifies the period after which a failed JMX connect is retried. "
			+ "The default is 10.0 seconds.";
	
	private static final String DESC_PARAM_RETRY_COUNT = 
			"Specifies the retry count for failed JMX connects. The default is "
			+ "-1, which means infinite retries.";
	
	private static final String DESC_PARAM_FILTER_DOCUMENT = 
			"Specifies the path to a JSON-formatted document that specifies "
			+ "the domain, instance, job, operator, and metric name filters as "
			+ "regular expressions. Each regular expression must follow the "
			+ "rules that are specified for Java "
			+ "[https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html|Pattern].";
	
	private static final String DESC_PARAM_SCAN_PERIOD = 
			"Specifies the period after which a new metrics scan is "
			+ "initiated. The default is 5.0 seconds.";

	private static final String DESC_PARAM_EMIT_METRIC_TUPLE =
			"Specifies when to emit a tuple for a metric. Supported modes are:\\n"
			+ "\\n"
			+ "* **periodic**\\n"
			+ "\\n"
			+ "  For each monitored metric a tuple is emitted during each scan cycle.\\n"
			+ "\\n"
			+ "* **onChangedValue** (default)\\n"
			+ "\\n"
			+ "  For each monitored metric a tuple is emitted during each scan cycle if the metric value changed.";
	
	// ------------------------------------------------------------------------
	// Implementation.
	// ------------------------------------------------------------------------
	
	/**
	 * Thread for calling <code>produceTuples()</code> to produce tuples 
	 */
	private Thread _processThread;

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(MetricsSource.class.getName());
	
	private OperatorConfiguration _operatorConfiguration = new OperatorConfiguration();
	
	private DomainHandler _domainHandler = null;
	
	@Parameter(
			optional=false,
			description=MetricsSource.DESC_PARAM_CONNECTION_URL
			)
	public void setConnectionURL(String connectionURL) {
		_operatorConfiguration.set_connectionURL(connectionURL);
	}

	@Parameter(
			optional=false,
			description=MetricsSource.DESC_PARAM_USER
			)
	public void setUser(String user) {
		_operatorConfiguration.set_user(user);
	}

	@Parameter(
			optional=false,
			description=MetricsSource.DESC_PARAM_PASSWORD
			)
	public void setPassword(String password) {
		_operatorConfiguration.set_password(password);
	}

	@Parameter(
			optional=true,
			description=MetricsSource.DESC_PARAM_DOMAIN_ID
			)
	public void setDomainId(String domainId) {
		_operatorConfiguration.set_domainId(domainId);
	}

	@Parameter(
			optional=true,
			description=MetricsSource.DESC_PARAM_RETRY_PERIOD
			)
	public void setRetryPeriod(double retryPeriod) {
		_operatorConfiguration.set_retryPeriod(retryPeriod);
	}

	@Parameter(
			optional=true,
			description=MetricsSource.DESC_PARAM_RETRY_COUNT
			)
	public void setRetryCount(int retryCount) {
		_operatorConfiguration.set_retryCount(retryCount);
	}

	@Parameter(
			optional=false,
			description=MetricsSource.DESC_PARAM_FILTER_DOCUMENT
			)
	public void setFilterDocument(String filterDocument) {
		_operatorConfiguration.set_filterDocument(filterDocument);
	}

	@Parameter(
			optional=true,
			description=MetricsSource.DESC_PARAM_SCAN_PERIOD
			)
	public void setScanPeriod(Double scanPeriod) {
		_operatorConfiguration.set_scanPeriod(scanPeriod);
	}

	@Parameter(
			optional=true,
			description=MetricsSource.DESC_PARAM_EMIT_METRIC_TUPLE
			)
	public void setEmitMetricTuple(EmitMetricTupleMode mode) {
		_operatorConfiguration.set_emitMetricTuple(mode);
	}

	/**
	 * Initialize this operator. Called once before any tuples are processed.
	 * @param context OperatorContext for this operator.
	 * @throws Exception Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
		// Must call super.initialize(context) to correctly setup an operator.
		super.initialize(context);
		_trace.trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );

		// Dynamic registration of additional class libraries to get rid of
		// the @Libraries annotation and its compile-time evaluation of
		// environment variables.
		setupClassPaths(context);
		
		/*
		 * The domainId parameter is optional. If the application developer does
		 * not set it, use the domain id under which the operator itself is
		 * running.
		 */
		if (_operatorConfiguration.get_domainId() == null) {
			if (context.getPE().isStandalone()) {
				throw new com.ibm.streams.operator.DataException("The " + context.getName() + " operator runs in standalone mode and can, therefore, not automatically determine a domain id.");
			}
			_operatorConfiguration.set_domainId(context.getPE().getDomainId());
			_trace.info("The " + context.getName() + " operator automatically connects to the " + _operatorConfiguration.get_domainId() + " domain.");
		}
		
		/*
		 * Establish connections or resources to communicate an external system
		 * or data store. The configuration information for this comes from
		 * parameters supplied to the operator invocation, or external
		 * configuration files or a combination of the two. 
		 */

		_operatorConfiguration.set_filters(Filters.setupFilters(_operatorConfiguration.get_filterDocument()));
		boolean isValidDomain = _operatorConfiguration.get_filters().matchesDomainId(_operatorConfiguration.get_domainId());
		if (!isValidDomain)
		{
			throw new com.ibm.streams.operator.DataException("The " + _operatorConfiguration.get_domainId() + " domain does not match the specified filter criteria in " + _operatorConfiguration.get_filterDocument());
		}
		
		/*
		 * Prepare the JMX environment settings.
		 */
		HashMap<String, Object> env = new HashMap<>();
		String [] credentials = { _operatorConfiguration.get_user(), _operatorConfiguration.get_password() };
		env.put("jmx.remote.credentials", credentials);
		env.put("jmx.remote.protocol.provider.pkgs", "com.ibm.streams.management");
		/*
		 * TODO streamtool getdomainproperty jmx.sslOption
		 * Code taken from:
		 * http://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.0/com.ibm.streams.dev.doc/doc/jmxapi-lgop.html
		 * 
		 * Is this needed? Seems to be not needed.
		 */
//		String sslOption = "TLSv1";
//		env.put("jmx.remote.tls.enabled.protocols", sslOption);

		/*
		 * Setup the JMX connector and MBean connection.
		 */
		_operatorConfiguration.set_jmxConnector(JMXConnectorFactory.connect(new JMXServiceURL(_operatorConfiguration.get_connectionURL()), env));
		_operatorConfiguration.set_mbeanServerConnection(_operatorConfiguration.get_jmxConnector().getMBeanServerConnection());

		/*
		 * Evaluate the output schema once.
		 * Create and submit tuples to the output port for each changed metric.
		 */
		final StreamingOutput<OutputTuple> port = getOutput(0);
		_operatorConfiguration.set_tupleContainer(new TupleContainer(port));
		
		/*
		 * Further actions are handled in the domain handler that manages
		 * instances that manages jobs, etc.
		 */
		_domainHandler = new DomainHandler(_operatorConfiguration, _operatorConfiguration.get_domainId());

		/*
		 * Create the thread for producing tuples. 
		 * The thread is created at initialize time but started.
		 * The thread will be started by allPortsReady().
		 */
		_processThread = getOperatorContext().getThreadFactory().newThread(
				new Runnable() {

					@Override
					public void run() {
						try {
							produceTuples();
						} catch (Exception e) {
							_trace.error("Operator error", e);
						}                    
					}

				});

		/*
		 * Set the thread not to be a daemon to ensure that the SPL runtime
		 * will wait for the thread to complete before determining the
		 * operator is complete.
		 */
		_processThread.setDaemon(false);
	}

	/**
	 * Notification that initialization is complete and all input and output ports 
	 * are connected and ready to receive and submit tuples.
	 * @throws Exception Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public synchronized void allPortsReady() throws Exception {
		OperatorContext context = getOperatorContext();
		_trace.trace("Operator " + context.getName() + " all ports are ready in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
		// Start a thread for producing tuples because operator 
		// implementations must not block and must return control to the caller.
		_processThread.start();
	}

	/**
	 * Submit new tuples to the output stream
	 * @throws Exception if an error occurs while submitting a tuple
	 */
	private void produceTuples() throws Exception  {
		boolean quit = false;
		while(!quit) {
			_domainHandler.captureMetrics();
			
			/*
			 * Emit a window marker after each scan cycle.
			 */
			_operatorConfiguration.get_tupleContainer().punctuate(Punctuation.WINDOW_MARKER);

			Thread.sleep(Double.valueOf(_operatorConfiguration.get_scanPeriod() * 1000.0).longValue());
		}

		/*
		 * When finished, submit a final punctuation:
		 */
		_operatorConfiguration.get_tupleContainer().punctuate(Punctuation.FINAL_MARKER);
	}

	/**
	 * Shutdown this operator, which will interrupt the thread
	 * executing the <code>produceTuples()</code> method.
	 * @throws Exception Operator failure, will cause the enclosing PE to terminate.
	 */
	public synchronized void shutdown() throws Exception {
		if (_processThread != null) {
			_processThread.interrupt();
			_processThread = null;
		}
		OperatorContext context = getOperatorContext();
		_trace.trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );

		// Close connections or release resources related to any external system or data store.
		
		if (_operatorConfiguration.get_jmxConnector() != null) {
			_operatorConfiguration.get_jmxConnector().close();
		}

		// Must call super.shutdown()
		super.shutdown();
	}

	/**
	 * Registers additional class libraries to get rid of the @Libraries
	 * annotation for the operator. Although the @Libraries annotation
	 * supports environment variables, these are evaluated during
	 * compile-time only requiring identical IBM Streams installation paths
	 * for the build and run-time environment.
	 *  
	 * @param context
	 * Context information for the Operator's execution context.
	 * 
	 * @throws Exception 
	 * Throws exception in case of unavailable STREAM_INSTALL environment
	 * variable, or problems while loading the required class libraries.
	 */
	private void setupClassPaths(OperatorContext context) throws Exception {
		final String STREAMS_INSTALL = System.getenv("STREAMS_INSTALL");
		if (STREAMS_INSTALL == null || STREAMS_INSTALL.isEmpty()) {
			throw new Exception("STREAMS_INSTALL environment variable must be set");
		}
		// See: http://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.0/com.ibm.streams.dev.doc/doc/jmxapi-start.html
		String[] libraries = {
				STREAMS_INSTALL + "/lib/com.ibm.streams.management.jmxmp.jar",
				STREAMS_INSTALL + "/lib/com.ibm.streams.management.mx.jar",
				STREAMS_INSTALL + "/ext/lib/jmxremote_optional.jar"
		};
		try {
			context.addClassLibraries(libraries);
		}
		catch(MalformedURLException e) {
			_trace.error("problem while adding class libraries: " + String.join(",", libraries), e);
			throw e;
		}
	}
}
