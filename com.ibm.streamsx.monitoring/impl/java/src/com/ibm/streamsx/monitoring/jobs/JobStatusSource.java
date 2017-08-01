//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.jobs;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.management.NotificationListener;
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
import com.ibm.streams.operator.control.patterns.NotificationTupleSubmitter;
import com.ibm.streams.operator.state.ConsistentRegionContext;
import com.ibm.streamsx.monitoring.jobs.internal.DomainHandler;
import com.ibm.streamsx.monitoring.jobs.internal.OperatorConfiguration;
import com.ibm.streamsx.monitoring.jobs.internal.TupleContainer;
import com.ibm.streamsx.monitoring.jobs.internal.filter.Filters;
import com.ibm.streamsx.monitoring.messages.Messages;

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
public class JobStatusSource extends AbstractOperator {

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
			+ "                \\\"operators\\\":\\n"
			+ "                [\\n"
			+ "                  {\\n"
			+ "                    \\\"operatorNamePatterns\\\":\\\".*\\\"\\n"
			+ "                  }\\n"
			+ "                ]\\n"
			+ "              }\\n"
			+ "            ]\\n"
			+ "          }\\n"
			+ "        ]\\n"
			+ "      }\\n"
			+ "    ]\\n"
			+ "\\n"			
			+ "The following filter document example selects events from the *Storage* operator in the *StreamsInstance* instance and *StreamsDomain* domain only:\\n"
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
			"The JobStatusSource operator emits a status tuple to this "
			+ "output port for each notification, for which the operator "
			+ "identifies a changed value. You can use the "
			+ "[type:com.ibm.streamsx.monitoring.jobs::JobStatusNotification|JobStatusNotification] "
			+ "tuple type, or any subset of the attributes specified for this type"
			;
	
	private static final String DESC_PARAM_APPLICATION_CONFIGURATION_NAME = 
			"Specifies the name of [https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.0/com.ibm.streams.admin.doc/doc/creating-secure-app-configs.html|application configuration object] "
			+ "that can contain connectionURL, user, password, and filterDocument "
			+ "properties. The application configuration overrides values that "
			+ "are specified with the corresponding parameters.";
	
	private static final String DESC_PARAM_CONNECTION_URL = 
			"Specifies the connection URL as returned by the `streamtool "
			+ "getjmxconnect` command. If the **applicationConfigurationName** "
			+ "parameter is specified, the application configuration can "
			+ "override this parameter value.";
	
	private static final String DESC_PARAM_USER = 
			"Specifies the user that is required for the JMX connection. If "
			+ "the **applicationConfigurationName** parameter is specified, "
			+ "the application configuration can override this parameter value.";
	
	private static final String DESC_PARAM_PASSWORD = 
			"Specifies the password that is required for the JMX connection. If "
			+ "the **applicationConfigurationName** parameter is specified, "
			+ "the application configuration can override this parameter value.";

	private static final String DESC_PARAM_SSL_OPTION = 
			"Specifies the sslOption that is required for the JMX connection. If "
			+ "the **applicationConfigurationName** parameter is specified, "
			+ "the application configuration can override this parameter value.";	
	
	private static final String DESC_PARAM_DOMAIN_ID = 
			"Specifies the domain id that is monitored. If no domain id is "
			+ "specified, the domain id under which this operator is running "
			+ "is used. If the operator is running in a standalone application "
			+ "it raises an exception and aborts. If "
			+ "the **applicationConfigurationName** parameter is specified, "
			+ "the application configuration can override this parameter value.";
	
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

	private static final Object PARAMETER_CONNECTION_URL = "connectionURL";
	
	private static final Object PARAMETER_USER = "user";

	private static final Object PARAMETER_PASSWORD = "password";
	
	private static final Object PARAMETER_SSL_OPTION = "sslOption";

	private static final Object PARAMETER_FILTER_DOCUMENT = "filterDocument";

	private static final String MISSING_VALUE = "The following value must be specified as parameter or in the application configuration: ";

	// ------------------------------------------------------------------------
	// Implementation.
	// ------------------------------------------------------------------------
	
	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(JobStatusSource.class.getName());
	
	private OperatorConfiguration _operatorConfiguration = new OperatorConfiguration();
	
	private DomainHandler _domainHandler = null;
	
	/**
	 * If the application configuration is used (applicationConfigurationName
	 * parameter is set), save the active filterDocument (as JSON string) to
	 * detect whether it changes between consecutive checks.
	 */
	private String activeFilterDocumentFromApplicationConfiguration = null;

	@Parameter(
			optional=true,
			description=JobStatusSource.DESC_PARAM_CONNECTION_URL
			)
	public void setConnectionURL(String connectionURL) {
		_operatorConfiguration.set_connectionURL(connectionURL);
	}

	@Parameter(
			optional=true,
			description=JobStatusSource.DESC_PARAM_USER
			)
	public void setUser(String user) {
		_operatorConfiguration.set_user(user);
	}

	@Parameter(
			optional=true,
			description=JobStatusSource.DESC_PARAM_PASSWORD
			)
	public void setPassword(String password) {
		_operatorConfiguration.set_password(password);
	}

	@Parameter(
			optional=true,
			description=JobStatusSource.DESC_PARAM_SSL_OPTION
			)
	public void setSslOption(String sslOption) {
		_operatorConfiguration.set_sslOption(sslOption);
	}
	
	@Parameter(
			optional=true,
			description=JobStatusSource.DESC_PARAM_DOMAIN_ID
			)
	public void setDomainId(String domainId) {
		_operatorConfiguration.set_domainId(domainId);
	}

	@Parameter(
			optional=true,
			description=JobStatusSource.DESC_PARAM_APPLICATION_CONFIGURATION_NAME
			)
	public void setApplicationConfigurationName(String applicationConfigurationName) {
		_operatorConfiguration.set_applicationConfigurationName(applicationConfigurationName);
	}

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

		setupFilters();
		boolean isValidDomain = _operatorConfiguration.get_filters().matchesDomainId(_operatorConfiguration.get_domainId());
		if (!isValidDomain) {
			throw new com.ibm.streams.operator.DataException("The " + _operatorConfiguration.get_domainId() + " domain does not match the specified filter criteria in " + _operatorConfiguration.get_filterDocument());
		}
		
		setupJMXConnection();

		_operatorConfiguration.set_tupleContainer(new TupleContainer(getOperatorContext(), getOutput(0)));
		
		/*
		 * Further actions are handled in the domain handler that manages
		 * instances that manages jobs, etc.
		 */
		scanDomain();

		/*
		 * Enable scheduled service for checking application configuration updates,
		 * if application configuration is configured.
		 */		
		if (_operatorConfiguration.get_applicationConfigurationName() != null) {
			java.util.concurrent.ScheduledExecutorService scheduler = getOperatorContext().getScheduledExecutorService();
			scheduler.scheduleWithFixedDelay(
					new Runnable() {
						@Override
						public void run() {
							try {
								detecteAndProcessChangedFilterDocumentInApplicationConfiguration();
							} catch (Exception e) {
								_trace.error("Operator error", e);
							}                    
						}
					}, 3000l, Double.valueOf(_operatorConfiguration.get_scanPeriod() * 1000.0).longValue(), TimeUnit.MILLISECONDS);
		}
		createAvoidCompletionThread();
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
					_operatorConfiguration.set_filters(Filters.setupFilters(inputStream));
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
			_operatorConfiguration.set_filters(Filters.setupFilters(filterDocument));
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
