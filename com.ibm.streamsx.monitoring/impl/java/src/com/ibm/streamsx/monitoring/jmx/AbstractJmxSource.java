//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.jmx;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OperatorContext.ContextCheck;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.ProcessingElement;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.model.Icons;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.compile.OperatorContextChecker;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.monitoring.jmx.OperatorConfiguration.OpType;
import com.ibm.streamsx.monitoring.jmx.internal.DomainHandler;
import com.ibm.streamsx.monitoring.jmx.internal.EmitMetricTupleMode;
import com.ibm.streamsx.monitoring.jmx.internal.JobStatusTupleContainer;
import com.ibm.streamsx.monitoring.jmx.internal.MetricsTupleContainer;
import com.ibm.streamsx.monitoring.jmx.internal.filters.Filters;
import com.ibm.streamsx.monitoring.messages.Messages;

/**
 * Abstract class for the JMX operators.
 */
public abstract class AbstractJmxSource extends AbstractOperator {

	// ------------------------------------------------------------------------
	// Documentation.
	// Attention: To add a newline, use \\n instead of \n.
	// ------------------------------------------------------------------------
	
	protected static final String DESC_PARAM_APPLICATION_CONFIGURATION_NAME = 
			"Specifies the name of [https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.0/com.ibm.streams.admin.doc/doc/creating-secure-app-configs.html|application configuration object] "
			+ "that can contain connectionURL, user, password, and filterDocument "
			+ "properties. The application configuration overrides values that "
			+ "are specified with the corresponding parameters.";
	
	protected static final String DESC_PARAM_CONNECTION_URL = 
			"Specifies the connection URL as returned by the `streamtool "
			+ "getjmxconnect` command. If the **applicationConfigurationName** "
			+ "parameter is specified, the application configuration can "
			+ "override this parameter value.";
	
	protected static final String DESC_PARAM_USER = 
			"Specifies the user that is required for the JMX connection. If "
			+ "the **applicationConfigurationName** parameter is specified, "
			+ "the application configuration can override this parameter value.";
	
	protected static final String DESC_PARAM_PASSWORD = 
			"Specifies the password that is required for the JMX connection. If "
			+ "the **applicationConfigurationName** parameter is specified, "
			+ "the application configuration can override this parameter value.";

	protected static final String DESC_PARAM_SSL_OPTION = 
			"Specifies the sslOption that is required for the JMX connection. If "
			+ "the **applicationConfigurationName** parameter is specified, "
			+ "the application configuration can override this parameter value.";	
	
	protected static final String DESC_PARAM_DOMAIN_ID = 
			"Specifies the domain id that is monitored. If no domain id is "
			+ "specified, the domain id under which this operator is running "
			+ "is used. If the operator is running in a standalone application "
			+ "it raises an exception and aborts. If "
			+ "the **applicationConfigurationName** parameter is specified, "
			+ "the application configuration can override this parameter value.";
	
	protected static final Object PARAMETER_CONNECTION_URL = "connectionURL";
	
	protected static final Object PARAMETER_USER = "user";

	protected static final Object PARAMETER_PASSWORD = "password";
	
	protected static final Object PARAMETER_SSL_OPTION = "sslOption";

	protected static final Object PARAMETER_FILTER_DOCUMENT = "filterDocument";

	protected static final String MISSING_VALUE = "The following value must be specified as parameter or in the application configuration: ";

	// ------------------------------------------------------------------------
	// Implementation.
	// ------------------------------------------------------------------------
	
	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(AbstractJmxSource.class.getName());
	
	protected OperatorConfiguration _operatorConfiguration = new OperatorConfiguration();
	
	protected DomainHandler _domainHandler = null;
	
	/**
	 * If the application configuration is used (applicationConfigurationName
	 * parameter is set), save the active filterDocument (as JSON string) to
	 * detect whether it changes between consecutive checks.
	 */
	protected String activeFilterDocumentFromApplicationConfiguration = null;

	@Parameter(
			optional=true,
			description=AbstractJmxSource.DESC_PARAM_CONNECTION_URL
			)
	public void setConnectionURL(String connectionURL) {
		_operatorConfiguration.set_connectionURL(connectionURL);
	}

	@Parameter(
			optional=true,
			description=AbstractJmxSource.DESC_PARAM_USER
			)
	public void setUser(String user) {
		_operatorConfiguration.set_user(user);
	}

	@Parameter(
			optional=true,
			description=AbstractJmxSource.DESC_PARAM_PASSWORD
			)
	public void setPassword(String password) {
		_operatorConfiguration.set_password(password);
	}

	@Parameter(
			optional=true,
			description=AbstractJmxSource.DESC_PARAM_SSL_OPTION
			)
	public void setSslOption(String sslOption) {
		_operatorConfiguration.set_sslOption(sslOption);
	}
	
	@Parameter(
			optional=true,
			description=AbstractJmxSource.DESC_PARAM_DOMAIN_ID
			)
	public void setDomainId(String domainId) {
		_operatorConfiguration.set_domainId(domainId);
	}

	@Parameter(
			optional=true,
			description=AbstractJmxSource.DESC_PARAM_APPLICATION_CONFIGURATION_NAME
			)
	public void setApplicationConfigurationName(String applicationConfigurationName) {
		_operatorConfiguration.set_applicationConfigurationName(applicationConfigurationName);
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

		setupFilters();
		boolean isValidDomain = _operatorConfiguration.get_filters().matchesDomainId(_operatorConfiguration.get_domainId());
		if (!isValidDomain) {
			throw new com.ibm.streams.operator.DataException("The " + _operatorConfiguration.get_domainId() + " domain does not match the specified filter criteria in " + _operatorConfiguration.get_filterDocument());
		}
		
		setupJMXConnection();

		final StreamingOutput<OutputTuple> port = getOutput(0);
		if (OpType.JOB_STATUS_SOURCE == _operatorConfiguration.get_OperatorType()) {
			_operatorConfiguration.set_tupleContainerJobStatusSource(new JobStatusTupleContainer(getOperatorContext(), port));
		}
		if (OpType.METRICS_SOURCE == _operatorConfiguration.get_OperatorType()) {			
			_operatorConfiguration.set_tupleContainerMetricsSource(new MetricsTupleContainer(port));
		}
		/*
		 * Further actions are handled in the domain handler that manages
		 * instances that manages jobs, etc.
		 */
		scanDomain();
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
	protected void setupClassPaths(OperatorContext context) throws Exception {
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

	/**
	 * Sets up a JMX connection. The connection URL, the user, and the password
	 * can be set with the operator parameters, or via application configuration.
	 * 
	 * @throws Exception
	 * Throws exception in case of invalid/bad URLs or other I/O problems.
	 */
	protected void setupJMXConnection() throws Exception {
		// Apply defaults, which are the parameter values.
		String connectionURL = _operatorConfiguration.get_connectionURL();
		String user = _operatorConfiguration.get_user();
		String password = _operatorConfiguration.get_password();
		String sslOption = "";
		// Override defaults if the application configuration is specified
		String applicationConfigurationName = _operatorConfiguration.get_applicationConfigurationName();
		if (applicationConfigurationName != null) {
			Map<String,String> properties = getApplicationConfiguration(applicationConfigurationName);
			if (properties.containsKey(PARAMETER_CONNECTION_URL)) {
				connectionURL = properties.get(PARAMETER_CONNECTION_URL);
			}
			if (properties.containsKey(PARAMETER_USER)) {
				user = properties.get(PARAMETER_USER);
			}
			if (properties.containsKey(PARAMETER_PASSWORD)) {
				password = properties.get(PARAMETER_PASSWORD);
			}
			if (properties.containsKey(PARAMETER_SSL_OPTION)) {
				sslOption = properties.get(PARAMETER_SSL_OPTION);
			}
		}
		// Ensure a valid configuration.
		if (connectionURL == null) {
			throw new Exception(MISSING_VALUE + PARAMETER_CONNECTION_URL);
		}
		if (user == null) {
			throw new Exception(MISSING_VALUE + PARAMETER_USER);
		}
		if (password == null) {
			throw new Exception(MISSING_VALUE + PARAMETER_PASSWORD);
		}
		/*
		 * Prepare the JMX environment settings.
		 */
		HashMap<String, Object> env = new HashMap<>();
		String [] credentials = { user, password };
		env.put("jmx.remote.credentials", credentials);
		env.put("jmx.remote.protocol.provider.pkgs", "com.ibm.streams.management");
		/*
		 * get the value from: streamtool getdomainproperty jmx.sslOption
		 * Code taken from:
		 * http://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.0/com.ibm.streams.dev.doc/doc/jmxapi-lgop.html
		 */
		if (sslOption != "") {
			env.put("jmx.remote.tls.enabled.protocols", sslOption);
		}

		/*
		 * Setup the JMX connector and MBean connection.
		 */
		_operatorConfiguration.set_jmxConnector(JMXConnectorFactory.connect(new JMXServiceURL(connectionURL), env));
		_operatorConfiguration.set_mbeanServerConnection(_operatorConfiguration.get_jmxConnector().getMBeanServerConnection());
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
				throw new Exception(MISSING_VALUE + PARAMETER_FILTER_DOCUMENT);
			}
			_operatorConfiguration.set_filters(Filters.setupFilters(filterDocument, _operatorConfiguration.get_OperatorType()));
		}
	}

	/**
	 * Calls the ProcessingElement.getApplicationConfiguration() method to
	 * retrieve the application configuration if application configuration
	 * is supported.
	 * 
	 * @return
	 * The application configuration.
	 */
	@SuppressWarnings("unchecked")
	protected Map<String,String> getApplicationConfiguration(String applicationConfigurationName) {
		Map<String,String> properties = null;
		try {
			ProcessingElement pe = getOperatorContext().getPE();
			Method method = ProcessingElement.class.getMethod("getApplicationConfiguration", new Class[]{String.class});
			Object returnedObject = method.invoke(pe, applicationConfigurationName);
			properties = (Map<String,String>)returnedObject;
		}
		catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			properties = new HashMap<>();
		}
		return properties;
	}
	
}
