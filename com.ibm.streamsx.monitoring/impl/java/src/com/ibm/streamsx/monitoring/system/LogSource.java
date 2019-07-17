//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.system;


import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.PrimitiveOperator;
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
		name="LogSource",
		namespace="com.ibm.streamsx.monitoring.system",
		description=LogSource.DESC_OPERATOR+AbstractJmxSource.AUTHENTICATION_DESC
		)
@OutputPorts({
	@OutputPortSet(
			cardinality=1,
			optional=false,
			windowPunctuationOutputMode=WindowPunctuationOutputMode.Free,
			description=LogSource.DESC_OUTPUT_PORT
			)
	,
	@OutputPortSet(
			cardinality=1,
			optional=true,
			windowPunctuationOutputMode=WindowPunctuationOutputMode.Free,
			description=AbstractJmxSource.DESC_OUTPUT_PORT_1
			)
})
public class LogSource extends AbstractJmxSource {

	// ------------------------------------------------------------------------
	// Documentation.
	// Attention: To add a newline, use \\n instead of \n.
	// ------------------------------------------------------------------------
	
	static final String DESC_OPERATOR = 
			"The LogSource operator uses the "
			+ "[http://www.ibm.com/support/knowledgecenter/SSCRJU_4.2.0/com.ibm.streams.ref.doc/doc/jmxapi.html|JMX] "
			+ "API to retrieve log events and provides "
			+ "log messages as tuple stream.\\n"
			+ "\\n"
			+ "The operator emits tuples for the following notification types:\\n"
			+ "* com.ibm.streams.management.log.application.error\\n"
			+ "* com.ibm.streams.management.log.application.warning\\n"
			+ "\\n"
			;
	
	protected static final String DESC_OUTPUT_PORT = 
			"The LogSource operator emits a status tuple to this "
			+ "output port for each notification, for which the operator "
			+ "identifies a changed value. You can use the "
			+ "[type:com.ibm.streamsx.monitoring.system::LogNotification|LogNotification] "
			+ "tuple type, or any subset of the attributes specified for this type"
			;

	// ------------------------------------------------------------------------
	// Implementation.
	// ------------------------------------------------------------------------
	private boolean _connected = true;

	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(LogSource.class.getName());


	@ContextCheck(compile = true)
	public static void checkInConsistentRegion(OperatorContextChecker checker) {
		//consistent region check
		OperatorContext oContext = checker.getOperatorContext();
		ConsistentRegionContext cContext = oContext.getOptionalContext(ConsistentRegionContext.class);
		if(cContext != null) {
			if(cContext.isStartOfRegion()) {
				checker.setInvalidContext(Messages.getString("CONSISTENT_CHECK"), new String[] {"LogSource"});
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
		_operatorConfiguration.set_OperatorType(OpType.LOG_SOURCE);
		super.initialize(context);

		/*
		 * Enable scheduled service for JMX connection health check
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
								scanInstance(); // create new InstanceHandler
							}
							if (_connected) {
								_instanceHandler.healthCheck();
							}
						}
						catch (Exception e) {
							_trace.error("JMX connection error ", e);
							_connected = false;
							closeInstanceHandler();
						}							
					}
				}, 3000l, Double.valueOf(5 * 1000.0).longValue(), TimeUnit.MILLISECONDS);
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
