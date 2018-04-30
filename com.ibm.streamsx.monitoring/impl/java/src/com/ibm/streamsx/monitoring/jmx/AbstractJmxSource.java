//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.jmx;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streamsx.monitoring.jmx.OperatorConfiguration.OpType;
import com.ibm.streamsx.monitoring.jmx.internal.ConnectionNotificationTupleContainer;
import com.ibm.streamsx.monitoring.jmx.internal.DomainHandler;
import com.ibm.streamsx.monitoring.jmx.internal.JobStatusTupleContainer;
import com.ibm.streamsx.monitoring.jmx.internal.LogTupleContainer;
import com.ibm.streamsx.monitoring.jmx.internal.MetricsTupleContainer;
import com.ibm.streamsx.monitoring.jmx.internal.filters.Filters;

/**
 * Abstract class for the JMX operators.
 */
public abstract class AbstractJmxSource extends AbstractJmxOperator {

	// ------------------------------------------------------------------------
	// Documentation.
	// Attention: To add a newline, use \\n instead of \n.
	// ------------------------------------------------------------------------
	
	
	public static final String DESC_OUTPUT_PORT_1 = 
			"Emits tuples containing JMX connection notifications.\\n"
			+ "The notification type is one of the following:\\n"
			+ "\\n"
			+ "* jmx.remote.connection.opened\\n"
			+ "* jmx.remote.connection.closed\\n"
			+ "* jmx.remote.connection.failed\\n"
			+ "* jmx.remote.connection.notifs.lost\\n"
			+ "\\n"
			+ "You can use the "
			+ "[type:com.ibm.streamsx.monitoring.jmx::ConnectionNotification|ConnectionNotification] "
			+ "tuple type, or any subset of the attributes specified for this "
			+ "type."
			;
	
	// ------------------------------------------------------------------------
	// Implementation.
	// ------------------------------------------------------------------------
	
	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(AbstractJmxSource.class.getName());
	
	protected DomainHandler _domainHandler = null;

	/**
	 * If the application configuration is used (applicationConfigurationName
	 * parameter is set), save the active filterDocument (as JSON string) to
	 * detect whether it changes between consecutive checks.
	 */
	protected String activeFilterDocumentFromApplicationConfiguration = null;

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

		/*
		 * Establish connections or resources to communicate an external system
		 * or data store. The configuration information for this comes from
		 * parameters supplied to the operator invocation, or external
		 * configuration files or a combination of the two. 
		 */
		if (OpType.LOG_SOURCE != _operatorConfiguration.get_OperatorType()) {
			setupFilters();
			boolean isValidDomain = _operatorConfiguration.get_filters().matchesDomainId(_operatorConfiguration.get_domainId());
			if (!isValidDomain) {
				throw new com.ibm.streams.operator.DataException("The " + _operatorConfiguration.get_domainId() + " domain does not match the specified filter criteria in " + _operatorConfiguration.get_filterDocument());
			}
		}		
		
		final StreamingOutput<OutputTuple> port = getOutput(0);
		if (OpType.JOB_STATUS_SOURCE == _operatorConfiguration.get_OperatorType()) {
			_operatorConfiguration.set_tupleContainerJobStatusSource(new JobStatusTupleContainer(getOperatorContext(), port));
		}
		if (OpType.LOG_SOURCE == _operatorConfiguration.get_OperatorType()) {
			_operatorConfiguration.set_tupleContainerLogSource(new LogTupleContainer(getOperatorContext(), port));
		}
		if (OpType.METRICS_SOURCE == _operatorConfiguration.get_OperatorType()) {			
			_operatorConfiguration.set_tupleContainerMetricsSource(new MetricsTupleContainer(port));
		}
		// check if second output port is present
		if (1 < context.getNumberOfStreamingOutputs()) {
			final StreamingOutput<OutputTuple> port1 = getOutput(1);
			_operatorConfiguration.set_tupleContainerConnectionNotification(new ConnectionNotificationTupleContainer(getOperatorContext(), port1));
		}
		
		setupJMXConnection();

		/*
		 * Further actions are handled in the domain handler that manages
		 * instances that manages jobs, etc.
		 */
		scanDomain();
	}	
	

	/**
	 * Detects whether the filterDocument in the application configuration
	 * changed if the applicationConfigurationName parameter is specified. 
	 * <p>
	 * @throws Exception 
	 * Throws in case of I/O issues or if the filter document is neither
	 * specified as parameter (file path), nor in the application configuration
	 * (JSON string).
	 */
	protected void detecteAndProcessChangedFilterDocumentInApplicationConfiguration() throws Exception {
		boolean isChanged = false;
		String applicationConfigurationName = _operatorConfiguration.get_applicationConfigurationName();
		if (applicationConfigurationName != null) {
			String filterDocument = getApplicationConfiguration(applicationConfigurationName).get(PARAMETER_FILTER_DOCUMENT);
			if (filterDocument != null) {
				if (activeFilterDocumentFromApplicationConfiguration == null) {
					isChanged = true;
				}
				else if (!activeFilterDocumentFromApplicationConfiguration.equals(filterDocument)) {
					isChanged = true;
				}
			}
			if (isChanged) {
				_domainHandler.close();
				_domainHandler = null;
				setupFilters();
				scanDomain();
			}
		}
	}
	
	protected void scanDomain() {
		_domainHandler = new DomainHandler(_operatorConfiguration, _operatorConfiguration.get_domainId());
	}

	/**
	 * Converts the path to absolute path.
	 */
	private String makeAbsolute(File rootForRelative, String path)
			throws IOException {
		File pathFile = new File(path);
		if (pathFile.isAbsolute()) {
			return pathFile.getCanonicalPath();
		} else {
			File abs = new File(rootForRelative.getAbsolutePath()
					+ File.separator + path);
			return abs.getCanonicalPath();
		}
	}	
	
	/**
	 * Setup the filters. The filters are either specified in an external text
	 * file (filterDocument parameter specifies the file path), or in the
	 * application control object as JSON string.
	 *  
	 * @throws Exception
	 * Throws in case of I/O issues or if the filter document is neither
	 * specified as parameter (file path), nor in the application configuration
	 * (JSON string).
	 */
	protected void setupFilters() throws Exception {
		boolean done = false;
		String applicationConfigurationName = _operatorConfiguration.get_applicationConfigurationName();
		if (applicationConfigurationName != null) {
			Map<String,String> properties = getApplicationConfiguration(applicationConfigurationName);
			if (properties.containsKey(PARAMETER_FILTER_DOCUMENT)) {
				String filterDocument = properties.get(PARAMETER_FILTER_DOCUMENT);
				_trace.debug("Detected modified filterDocument in application configuration: " + filterDocument);
				String filterDoc = filterDocument.replaceAll("\\\\t", ""); // remove tabs
				try(InputStream inputStream = new ByteArrayInputStream(filterDoc.getBytes())) {
					_operatorConfiguration.set_filters(Filters.setupFilters(inputStream, _operatorConfiguration.get_OperatorType()));
					activeFilterDocumentFromApplicationConfiguration = filterDocument; // save origin document
					done = true;
				}
			}
		}
		if (!done) {
			// The filters are not specified in the application configuration.
			String filterDocument = _operatorConfiguration.get_filterDocument();
			if (filterDocument == null) {
				filterDocument = _operatorConfiguration.get_defaultFilterDocument();
				_trace.info("filterDocument is not specified, use default: " + filterDocument);
			}
			else {
				_trace.debug("filterDocument is not in application configuration:" + filterDocument);
			}
			String fileAbsolute = makeAbsolute(this.baseDir, filterDocument);
			File fdoc = new File(fileAbsolute);
			if (fdoc.exists()) {
				_operatorConfiguration.set_filters(Filters.setupFilters(fileAbsolute, _operatorConfiguration.get_OperatorType()));
			}
			else {
				_trace.debug("filterDocument is not a file");
				String filterDoc = filterDocument.replaceAll("\\\\t", ""); // remove tabs
				try(InputStream inputStream = new ByteArrayInputStream(filterDoc.getBytes())) {
					_operatorConfiguration.set_filters(Filters.setupFilters(inputStream, _operatorConfiguration.get_OperatorType()));
				}				
			}
		}
	}

	protected void closeDomainHandler() {
		try {
			_domainHandler.close();
		}
		catch (Exception ignore) {
		}
		_domainHandler = null;
		if (1 == get_isConnected().getValue()) {
			// update metric to indicate connection is broken
			get_nBrokenJMXConnections().increment();
			// update metric to indicate that we are not connected
			get_isConnected().setValue(0);
		}
	}
	
	
}
