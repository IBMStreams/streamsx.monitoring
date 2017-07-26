//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.system;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;

/**
 * A source operator that does not receive any input streams and produces new
 * tuples. The method <code>produceTuples</code> is called to begin submitting
 * tuples.
 * <P>
 * For a source operator, the following event methods from the Operator
 * interface can be called:
 * </p>
 * <ul>
 * <li><code>initialize()</code> to perform operator initialization</li>
 * <li>allPortsReady() notification indicates the operator's ports are ready to
 * process and submit tuples</li>
 * <li>shutdown() to shutdown the operator. A shutdown request may occur at any
 * time, such as a request to stop a PE or cancel a job. Thus the shutdown() may
 * occur while the operator is processing tuples, punctuation marks, or even
 * during port ready notification.</li>
 * </ul>
 * <p>
 * With the exception of operator initialization, all the other events may occur
 * concurrently with each other, which lead to these methods being called
 * concurrently by different threads.
 * </p>
 */
@PrimitiveOperator(name = "SystemMonitorSource", namespace = "com.ibm.streamsx.monitoring.system", description = "The Java Operator SystemMonitorSource collects the CPU and Memory usage from the current running system and emits a tuple in an interval.")
@OutputPorts({
		@OutputPortSet(description = "Port that produces tuples with the schema SystemStatus", cardinality = 1, optional = false, windowPunctuationOutputMode = WindowPunctuationOutputMode.Generating) })
public class SystemMonitorSource extends AbstractOperator {

	/**
	 * period This mandatory parameter specifies the time interval between
	 * successive tuple submissions, in seconds. It takes a single expression of
	 * type int32 as its value. When not specified, it is assumed to be 5.
	 */
	protected int period = 5;

	/**
	 * cpuUsage The optional Boolean parameter cpuUsage is to enable or disable
	 * the CPU Usage information in results.(default true)."
	 */
	protected boolean cpuUsage = true;

	/**
	 * memUsage The optional Boolean parameter cpuUsage is to enable or disable
	 * the Memory Usage information in results.(default true)."
	 */
	protected boolean memUsage = true;

	/**
	 * detail The optional Boolean parameter detail is to enable or disable the
	 * details of results.(default false)."
	 */
	protected boolean detail = false;

	private boolean shutdown = false;

	private Thread processThread;

	/**
	 * Initialize this operator. Called once before any tuples are processed.
	 * 
	 * @param context
	 *            OperatorContext for this operator.
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public synchronized void initialize(OperatorContext context) throws Exception {
		// Must call super.initialize(context) to correctly setup an operator.
		super.initialize(context);
		Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " initializing in PE: "
				+ context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());

		/*
		 * Create the thread for producing tuples. The thread is created at
		 * initialize time but started. The thread will be started by
		 * allPortsReady().
		 */
		processThread = getOperatorContext().getThreadFactory().newThread(new Runnable() {

			@Override
			public void run() {
				try {
					produceTuples();
				} catch (Exception e) {
					Logger.getLogger(this.getClass()).error("Operator error", e);
				}
			}

		});

		/*
		 * Set the thread not to be a daemon to ensure that the SPL runtime will
		 * wait for the thread to complete before determining the operator is
		 * complete.
		 */
		processThread.setDaemon(false);
	}

	@Parameter(optional = false, description = "period This mandatory parameter specifies the time interval between successive tuple submissions, in seconds. It takes a single expression of type integer as its value. The default period value is set to 5 if parameter is not specified.")
	public void setPeriod(int value) {
		this.period = value;
	}

	@Parameter(optional = true, description = "The optional Boolean parameter cpuUsage is to enable or disable the CPU usage request. (default true).")
	public void setCpuUsage(boolean value) {
		this.cpuUsage = value;
	}

	@Parameter(optional = true, description = "The optional Boolean parameter memUsage is to enable or disable the memory usage request.(default true).")
	public void setMemUsage(boolean value) {
		this.memUsage = value;
	}

	@Parameter(optional = true, description = "The optional Boolean parameter detail is to enable or disable the details of results.(default false). If set to true, then the top command is used to retrieve the value and the status attribute contains additional information.")
	public void setDetail(boolean value) {
		this.detail = value;
	}

	/**
	 * Notification that initialization is complete and all input and output
	 * ports are connected and ready to receive and submit tuples.
	 * 
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public synchronized void allPortsReady() throws Exception {
		OperatorContext context = getOperatorContext();
		Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " all ports are ready in PE: "
				+ context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());
		// Start a thread for producing tuples because operator
		// implementations must not block and must return control to the caller.
		processThread.start();
	}

	/**
	 * Submit new tuples to the output stream
	 * 
	 * @throws Exception
	 *             if an error occurs while submitting a tuple
	 */
	private void produceTuples() throws Exception {
		final StreamingOutput<OutputTuple> out = getOutput(0);

		OutputTuple tuple = out.newTuple();
		while (!shutdown) {
			SystemStatus systemStatus = GetSystemStatus.getCpuMemUsage(detail);
			// Set attributes in tuple:
			tuple.setString("status", systemStatus.date);
			tuple.setFloat("cpuUsage", systemStatus.CpuUsage);
			tuple.setFloat("memUsage", systemStatus.MemUsage);
			tuple.setInt("memTotal", systemStatus.MemTotal);
			tuple.setInt("memUsed", systemStatus.MemUsed);
			// Submit tuple to output stream
			out.submit(tuple);
			Thread.sleep(period * 1000);
		}
	}

	/**
	 * Shutdown this operator, which will interrupt the thread executing the
	 * <code>produceTuples()</code> method.
	 * 
	 * @throws Exception
	 *             Operator failure, will cause the enclosing PE to terminate.
	 */
	public synchronized void shutdown() throws Exception {
		shutdown = true;
		if (processThread != null) {
			processThread.interrupt();
			processThread = null;

			// server.disconnect();
			super.shutdown();

		}
		OperatorContext context = getOperatorContext();
		Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: "
				+ context.getPE().getPEId() + " in Job: " + context.getPE().getJobId());

		// Must call super.shutdown()
		super.shutdown();
	}
}
