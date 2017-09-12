//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.jmx;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.ProcessingElement;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streamsx.monitoring.jmx.OperatorConfiguration.OpType;
import com.ibm.streamsx.monitoring.jmx.internal.DomainHandler;
import com.ibm.streamsx.monitoring.jmx.internal.JobStatusTupleContainer;
import com.ibm.streamsx.monitoring.jmx.internal.LogTupleContainer;
import com.ibm.streamsx.monitoring.jmx.internal.MetricsTupleContainer;
import com.ibm.streamsx.monitoring.jmx.internal.filters.Filters;

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
			+ "override this parameter value."
			+ "If not specified and the domainId parameter value equals the domain "
			+ "id under which this operator is running, then the operator uses the "
			+ "`streamtool getjmxconnect` command to get the value.";
	
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
			+ "the application configuration can override this parameter value."
			+ "If not specified and the domainId parameter value equals the domain "
			+ "id under which this operator is running, then the operator uses the "
			+ "`streamtool getdomainproperty` command to get the value.";
	
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
	
	protected static final Object PARAMETER_DOMAIN_ID = "domainId";

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
	
	private String _domainId = null; // domainId for this PE
	
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
			String domainId = getApplicationConfigurationDomainId();
			if (domainId != "") {
				_operatorConfiguration.set_domainId(domainId);
				_trace.info("The " + context.getName() + " operator connects to the " + _operatorConfiguration.get_domainId() + " domain specified by application configuration.");				
			}
			else {
				_operatorConfiguration.set_domainId(context.getPE().getDomainId());
				_trace.info("The " + context.getName() + " operator automatically connects to the " + _operatorConfiguration.get_domainId() + " domain.");
			}
		}
		_domainId = context.getPE().getDomainId();

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
		
		setupJMXConnection();

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
				STREAMS_INSTALL + "/ext/lib/jmxremote_optional.jar",
				STREAMS_INSTALL + "/ext/lib/JSON4J.jar"
		};
		try {
			context.addClassLibraries(libraries);
		}
		catch(MalformedURLException e) {
			_trace.error("problem while adding class libraries: " + String.join(",", libraries), e);
			throw e;
		}
	}
	
	protected String getApplicationConfigurationDomainId() {
		String result = "";
		String applicationConfigurationName = _operatorConfiguration.get_applicationConfigurationName();
		if (applicationConfigurationName != null) {
			Map<String,String> properties = getApplicationConfiguration(applicationConfigurationName);
			if (properties.containsKey(PARAMETER_DOMAIN_ID)) {
				result = properties.get(PARAMETER_DOMAIN_ID);
			}
		}
		return result;
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
		String sslOption = _operatorConfiguration.get_sslOption();
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
			// not configured via parameter or application configuration
			connectionURL = autoDetectJmxConnect();
			if (connectionURL == null) {
				throw new Exception(MISSING_VALUE + PARAMETER_CONNECTION_URL);
			}
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
		if (sslOption != null) {
			env.put("jmx.remote.tls.enabled.protocols", sslOption);
		}
		else {
			// not configured via parameter or application configuration
			sslOption = autoDetectJmxSslOption(user, password);
			if (sslOption != null) {
				env.put("jmx.remote.tls.enabled.protocols", sslOption);
			}
		}

		/*
		 * Setup the JMX connector and MBean connection.
		 */
		String[] urls = connectionURL.split(","); // comma separated list of JMX servers is supported
		// In Streaming Analytics service the variable urls contains 3 JMX servers. The third JMX is the prefered one. 
		for (int i=urls.length-1; i>=0; i--) {
			try {
				_trace.info("Connect to : " + urls[i]);
				_operatorConfiguration.set_jmxConnector(JMXConnectorFactory.connect(new JMXServiceURL(urls[i]), env));
				break; // exit loop here since a valid connection is established, otherwise exception is thrown.
			} catch (IOException e) {
				_trace.warn("Exception: " + e.getMessage());
				if (i == 0) {
					throw e;
				}
			}
		}
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
	
	private String autoDetectJmxConnect() {
		String result = null;
		if (_operatorConfiguration.get_domainId() == _domainId) { // running in same domain as configured
			_trace.debug("get connectionURL with streamtool getjmxconnect");
			String cmd = "streamtool getjmxconnect -d "+ _domainId;

			StringBuffer output = new StringBuffer();
			Process p;
			try {
				p = Runtime.getRuntime().exec(cmd);
				p.waitFor();
				BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
	            String line = "";
				while ((line = br.readLine())!= null) {
					output.append(line).append(",");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			result = output.toString();

			_trace.info("connectionURL=[" + result + "]");
			_operatorConfiguration.set_connectionURL(result);
		}
		return result;
	}	

	private String autoDetectJmxSslOption(String user, String password) {		
		String sslOption = null;
		if (_operatorConfiguration.get_domainId() == _domainId) { // running in same domain as configured
			_trace.debug("get jmx.sslOption with streamtool getdomainproperty");
	
			String[] cmd = { "/bin/sh", "-c", "echo "+password+" | streamtool getdomainproperty -d "+ _domainId + " -U "+user+" jmx.sslOption" };
			
			StringBuffer output = new StringBuffer();
			Process p;
			try {
				p = Runtime.getRuntime().exec(cmd);
				p.waitFor();
				BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
	            String line = "";
				while ((line = br.readLine())!= null) {
					output.append(line);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			String cmdResult = output.toString();
			_trace.debug("cmdResult: " + cmdResult);
			int idx = cmdResult.indexOf("=");
			if (idx >=0) {
				sslOption = cmdResult.substring(idx+1, cmdResult.length());
				_trace.info("sslOption=[" + sslOption + "]");
			}
		}
		return sslOption;
	}	
	
}
