//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.metrics;


import org.apache.log4j.Logger;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.monitoring.messages.Messages;
import com.ibm.streamsx.monitoring.jmx.AbstractJmxSource;
import com.ibm.streamsx.monitoring.jmx.OperatorConfiguration.OpType;
import com.ibm.streamsx.monitoring.jmx.internal.EmitMetricTupleMode;

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
		namespace="com.ibm.streamsx.monitoring.metrics",
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
public class MetricsSource extends AbstractJmxSource {

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
			+ "Per default, the MetricsSource operator monitors all metrics "
			+ "in the current domain and instance, if filter document is not specified."
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
			+ "Only those objects (and their parents) that "
			+ "match the specified filters, are monitored.\\n"
			+ "\\n"
			+ "It also specifies "
			+ "which name patterns are related, for example: For a domain X "
			+ "monitor all instances, whereas in each instance only jobs with "
			+ "a Y in their names shall be evaluated. For another domain Z, "
			+ "only jobs with a name ending with XYZ, are monitored, etc.\\n"
			+ "\\n"	
			+ "The filter document is a JSON-encoded text file or JSON-encoded String that is "
			+ "configured with the **filterDocument** parameter.\\n"
			+ "\\n"
			+ "If the **applicationConfigurationName** parameter is specified, "
			+ "the application configuration can override the **filterDocument** parameter value."
			+ "\\n"			
			+ "The JSON-formatted document specifies filters as regular expressions.\\n"
			+ "Each regular expression must follow the "
			+ "rules that are specified for Java "
			+ "[https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html|Pattern].\\n" 			
			+ "\\n"		
			+ "The following filter document example specifies to collect all PE and operator metrics available in all jobs and instances:\\n"
			+ "\\n"			
			+ "    [\\n"
			+ "      {\\n"
			+ "        \\\"domainIdPatterns\\\":\\\".*\\\",\\n"
			+ "        \\\"instances\\\":\\n"
			+ "        [\\n"
			+ "          {\\n"
			+ "            \\\"instanceIdPatterns\\\":\\\".*\\\",\\n"
			+ "            \\\"jobs\\\":\\n"
			+ "            [\\n"
			+ "              {\\n"
			+ "                \\\"jobNamePatterns\\\":\\\".*\\\",\\n"
			+ "                \\\"pes\\\":\\n"
			+ "                [\\n"
			+ "                  {\\n"
			+ "                    \\\"metricNamePatterns\\\":\\\".*\\\",\\n"
			+ "                    \\\"inputPorts\\\":\\n"
			+ "                    [\\n"
			+ "                      {\\n"
			+ "                        \\\"portIndexes\\\":\\\"*\\\",\\n"
			+ "                        \\\"metricNamePatterns\\\":\\\".*\\\"\\n"
			+ "                      }\\n"
			+ "                    ],\\n"
			+ "                    \\\"outputPorts\\\":\\n"
			+ "                    [\\n"
			+ "                      {\\n"
			+ "                        \\\"portIndexes\\\":\\\"*\\\",\\n"
			+ "                        \\\"metricNamePatterns\\\":\\\".*\\\"\\n"
			+ "                      }\\n"
			+ "                    ],\\n"
			+ "                    \\\"connections\\\":\\n"
			+ "                    [\\n"
			+ "                      {\\n"
			+ "                        \\\"connectionIdPatterns\\\":\\\".*\\\",\\n"
			+ "                        \\\"metricNamePatterns\\\":\\\".*\\\"\\n"
			+ "                      }\\n"
			+ "                    ]\\n"
			+ "                  }\\n"
			+ "                ],\\n"
			+ "                \\\"operators\\\":\\n"
			+ "                [\\n"
			+ "                  {\\n"
			+ "                    \\\"operatorNamePatterns\\\":\\\".*\\\",\\n"
			+ "                    \\\"metricNamePatterns\\\":\\\".*\\\",\\n"
			+ "                    \\\"inputPorts\\\":\\n"
			+ "                    [\\n"
			+ "                      {\\n"
			+ "                        \\\"portIndexes\\\":\\\"*\\\",\\n"
			+ "                        \\\"metricNamePatterns\\\":\\\".*\\\"\\n"
			+ "                      }\\n"
			+ "                    ],\\n"
			+ "                    \\\"outputPorts\\\":\\n"
			+ "                    [\\n"
			+ "                      {\\n"
			+ "                        \\\"portIndexes\\\":\\\"*\\\",\\n"
			+ "                        \\\"metricNamePatterns\\\":\\\".*\\\"\\n"
			+ "                      }\\n"
			+ "                    ]\\n"
			+ "                  }\\n"
			+ "                ]\\n"
			+ "              }\\n"
			+ "            ]\\n"
			+ "          }\\n"
			+ "        ]\\n"
			+ "      }\\n"
			+ "    ]\\n"
			+ "\\n"			
			+ "The following filter document example selects two custom metrics names *nBytesWritten* and *nObjects* of a custom *Storage* operator in the *StreamsInstance* instance and *StreamsDomain* domain only:\\n"
			+ "\\n"			
			+ "    [\\n"
			+ "      {\\n"
			+ "        \\\"domainIdPatterns\\\":\\\"StreamsDomain\\\",\\n"
			+ "        \\\"instances\\\":\\n"
			+ "        [\\n"
			+ "          {\\n"
			+ "            \\\"instanceIdPatterns\\\":\\\"StreamsInstance\\\",\\n"
			+ "            \\\"jobs\\\":\\n"
			+ "            [\\n"
			+ "              {\\n"
			+ "                \\\"jobNamePatterns\\\":\\\".*\\\",\\n"
			+ "                \\\"operators\\\":\\n"
			+ "                [\\n"
			+ "                  {\\n"
			+ "                    \\\"operatorNamePatterns\\\":\\\"Storage\\\",\\n"
			+ "                    \\\"metricNamePatterns\\\":\\n"
			+ "                    [\\n"
			+ "                      \\\"nBytesWritten\\\",\\n"
			+ "                      \\\"nObjects\\\"\\n"
			+ "                    ]\\n"
			+ "                  }\\n"
			+ "                ]\\n"
			+ "              }\\n"
			+ "            ]\\n"
			+ "          }\\n"
			+ "        ]\\n"
			+ "      }\\n"
			+ "    ]\\n"
			+ "\\n"
			;
	
	protected static final String DESC_OUTPUT_PORT = 
			"The MetricsSource operator emits a metric tuple to this "
			+ "output port for each metric, for which the operator "
			+ "identifies a changed value. You can use the "
			+ "[type:com.ibm.streamsx.monitoring.metrics::Notification|Notification] "
			+ "tuple type, or any subset of the attributes specified for this "
			+ "type. After each scan cycle, the operator emits a WindowMarker "
			+ "to this port."
			;
	
	private static final String DESC_PARAM_FILTER_DOCUMENT = 
			"Specifies the either a path to a JSON-formatted document or a JSON-formatted String that specifies "
			+ "the domain, instance, job, operator, and metric name filters as "
			+ "regular expressions. Each regular expression must follow the "
			+ "rules that are specified for Java "
			+ "[https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html|Pattern]. "
			+ "If the **applicationConfigurationName** parameter is specified, "
			+ "the application configuration can override this parameter value.";
	
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

	@Parameter(
			optional=true,
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

	@ContextCheck(compile = true)
	public static void checkInConsistentRegion(OperatorContextChecker checker) {
		//consistent region check
		OperatorContext oContext = checker.getOperatorContext();
		ConsistentRegionContext cContext = oContext.getOptionalContext(ConsistentRegionContext.class);
		if(cContext != null) {
			if(cContext.isStartOfRegion()) {
				checker.setInvalidContext(Messages.getString("CONSISTENT_CHECK"), new String[] {"MetricsSource"});
			}
		}		
	}

	/**
	 * Initialize this operator. Called once before any tuples are processed.
	 * @param context OperatorContext for this operator.
	 * @throws Exception Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
		_trace.trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
		_operatorConfiguration.set_OperatorType(OpType.METRICS_SOURCE);
		super.initialize(context);
		
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
		boolean connected = true;
		while(!quit) {
			
			try {
				if (!connected) {
					_trace.warn("Reconnect");
					setupJMXConnection();
					connected = true;
					scanDomain(); // create new DomainHandler
				}

				detecteAndProcessChangedFilterDocumentInApplicationConfiguration();

				if (connected) {
					_domainHandler.healthCheck();					
					_domainHandler.captureMetrics();
				}
			}
			catch (Exception e) {
				_trace.error("JMX connection error ", e);
				connected = false;
				closeDomainHandler();
				setupFilters();
			}
			/*
			 * Emit a window marker after each scan cycle.
			 */
			_operatorConfiguration.get_tupleContainerMetricsSource().punctuate(Punctuation.WINDOW_MARKER);

			Thread.sleep(Double.valueOf(_operatorConfiguration.get_scanPeriod() * 1000.0).longValue());
		}

		/*
		 * When finished, submit a final punctuation:
		 */
		_operatorConfiguration.get_tupleContainerMetricsSource().punctuate(Punctuation.FINAL_MARKER);
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

}
