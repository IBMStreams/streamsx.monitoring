//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.jobs;


import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.monitoring.jmx.AbstractJmxSource;
import com.ibm.streamsx.monitoring.jmx.OperatorConfiguration.OpType;
import com.ibm.streamsx.monitoring.messages.Messages;
import java.util.concurrent.TimeUnit;

/**
 * A source operator that does not receive any input streams and produces new tuples. 
 * Tuples are submitted when receiving JMX notifications.
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
		name="JobStatusSource",
		namespace="com.ibm.streamsx.monitoring.jobs",
		description=JobStatusSource.DESC_OPERATOR
		)
@OutputPorts({
	@OutputPortSet(
			cardinality=1,
			optional=false,
			windowPunctuationOutputMode=WindowPunctuationOutputMode.Free,
			description=JobStatusSource.DESC_OUTPUT_PORT
			)
})
public class JobStatusSource extends AbstractJmxSource {

	// ------------------------------------------------------------------------
	// Documentation.
	// Attention: To add a newline, use \\n instead of \n.
	// ------------------------------------------------------------------------
	
	static final String DESC_OPERATOR = 
			"The JobStatusSource operator uses the "
			+ "[http://www.ibm.com/support/knowledgecenter/SSCRJU_4.2.0/com.ibm.streams.ref.doc/doc/jmxapi.html|JMX] "
			+ "API to retrieve status updates from one or more jobs, and provides "
			+ "status changes as tuple stream.\\n"
			+ "\\n"
			+ "The operator emits tuples for the following notification types:\\n"
			+ "* com.ibm.streams.management.job.added\\n"
			+ "* com.ibm.streams.management.job.removed\\n"
			+ "* com.ibm.streams.management.pe.changed\\n"			
			+ "\\n"
			+ "As an application developer, you provide the so-called filter "
			+ "document. The filter document specifies patterns for domain, "
			+ "instance, job and operator names. It also specifies "
			+ "which name patterns are related, for example: For a domain X "
			+ "monitor all instances, whereas in each instance only jobs with "
			+ "a Y in their names shall be evaluated. For another domain Z, "
			+ "only jobs with a name ending with XYZ, are monitored, etc.\\n"
			+ "\\n"
			+ "Per default, the JobStatusSource operator monitors neither any "
			+ "domain, nor any instances, nor job, nor any other Streams job-"
			+ "related object. Only those objects (and their parents) that "
			+ "match the specified filters, are monitored.\\n"
			+ "\\n"
			+ "The JobStatusSource operator monitors filter-matching domains, "
			+ "instances, and jobs that are running while the application that "
			+ "uses the JobStatusSource operator, starts. Furthermore, the "
			+ "operator gets notifications for created and deleted instances, "
			+ "and submitted and cancelled jobs. Thererfore, the operator can "
			+ "retrieve events from filter-matching jobs that are submitted "
			+ "in the future.\\n"
			+ "\\n"
			+ "+ Filter document\\n"
			+ "\\n"
			+ "The filter document specifies patterns for domain, instance, "
			+ "job and operator names, and their relations.\\n"
			+ "\\n"
			+ "It also specifies "
			+ "which name patterns are related, for example: For a domain X "
			+ "monitor all instances, whereas in each instance only jobs with "
			+ "a Y in their names shall be evaluated. For another domain Z, "
			+ "only jobs with a name ending with XYZ, are monitored, etc.\\n"
			+ "\\n"	
			+ "The filter document is a JSON-encoded text file that is "
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
			+ "The following filter document example specifies to collect all PE status events in all jobs and instances:\\n"
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
			+ "              }\\n"
			+ "            ]\\n"
			+ "          }\\n"
			+ "        ]\\n"
			+ "      }\\n"
			+ "    ]\\n"
			+ "\\n"			
			+ "The following filter document example selects events from jobs in the *StreamsInstance* instance and *StreamsDomain* domain only:\\n"
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
			+ "              }\\n"
			+ "            ]\\n"
			+ "          }\\n"
			+ "        ]\\n"
			+ "      }\\n"
			+ "    ]\\n"
			+ "\\n"
			;
	
	protected static final String DESC_OUTPUT_PORT = 
			"The JobStatusSource operator emits a status tuple to this "
			+ "output port for each notification, for which the operator "
			+ "identifies a changed value. You can use the "
			+ "[type:com.ibm.streamsx.monitoring.jobs::JobStatusNotification|JobStatusNotification] "
			+ "tuple type, or any subset of the attributes specified for this type"
			;

	private static final String DESC_PARAM_FILTER_DOCUMENT = 
			"Specifies the path to a JSON-formatted document that specifies "
			+ "the domain, instance, job and operator filters as "
			+ "regular expressions. Each regular expression must follow the "
			+ "rules that are specified for Java "
			+ "[https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html|Pattern]. "
			+ "If the **applicationConfigurationName** parameter is specified, "
			+ "the application configuration can override this parameter value.";
	
	private static final String DESC_PARAM_SCAN_PERIOD = 
			"Specifies the period after which is checked if the application configuration is updated."
			+ "The default is 5.0 seconds.";

	// ------------------------------------------------------------------------
	// Implementation.
	// ------------------------------------------------------------------------
	
	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(JobStatusSource.class.getName());
	
	private boolean _connected = true;
	
	@Parameter(
			optional=true,
			description=JobStatusSource.DESC_PARAM_FILTER_DOCUMENT
			)
	public void setFilterDocument(String filterDocument) {
		_operatorConfiguration.set_filterDocument(filterDocument);
	}

	@Parameter(
			optional=true,
			description=JobStatusSource.DESC_PARAM_SCAN_PERIOD
			)
	public void setScanPeriod(Double scanPeriod) {
		_operatorConfiguration.set_scanPeriod(scanPeriod);
	}

	@ContextCheck(compile = true)
	public static void checkInConsistentRegion(OperatorContextChecker checker) {
		//consistent region check
		OperatorContext oContext = checker.getOperatorContext();
		ConsistentRegionContext cContext = oContext.getOptionalContext(ConsistentRegionContext.class);
		if(cContext != null) {
			if(cContext.isStartOfRegion()) {
				checker.setInvalidContext(Messages.getString("CONSISTENT_CHECK"), new String[] {"JobStatusSource"});
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
		_operatorConfiguration.set_OperatorType(OpType.JOB_STATUS_SOURCE);
		super.initialize(context);

		/*
		 * Enable scheduled service for checking application configuration updates
		 * and for JMX connection health check
		 */				
		java.util.concurrent.ScheduledExecutorService scheduler = getOperatorContext().getScheduledExecutorService();
		scheduler.scheduleWithFixedDelay(
				new Runnable() {
					@Override
					public void run() {
						try {
							if (!_connected) {
								_trace.warn("Reconnect");
								setupJMXConnection();
								_connected = true;
								scanDomain(); // create new DomainHandler
							}

							if (_connected) {
								_domainHandler.healthCheck();
								
								if (_operatorConfiguration.get_applicationConfigurationName() != null) {
									detecteAndProcessChangedFilterDocumentInApplicationConfiguration();
								}
							}
						}
						catch (Exception e) {
							_trace.error("JMX connection error ", e);
							_connected = false;
							closeDomainHandler();
						}							
					}
				}, 3000l, Double.valueOf(_operatorConfiguration.get_scanPeriod() * 1000.0).longValue(), TimeUnit.MILLISECONDS);
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
	}

	/**
	 * Shutdown this operator.
	 * @throws Exception Operator failure, will cause the enclosing PE to terminate.
	 */
	public synchronized void shutdown() throws Exception {
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
