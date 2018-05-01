//
// ****************************************************************************
// * Copyright (C) 2016, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.jmx;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.log4j.Logger;

import com.ibm.json.java.JSON;
import com.ibm.json.java.JSONArtifact;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.ProcessingElement;
import com.ibm.streams.operator.metrics.Metric;
import com.ibm.streams.operator.metrics.Metric.Kind;
import com.ibm.streams.operator.model.CustomMetric;
import com.ibm.streams.operator.model.Parameter;

/**
 * Abstract class for the JMX operators.
 */
public abstract class AbstractJmxOperator extends AbstractOperator {

	// ------------------------------------------------------------------------
	// Documentation.
	// Attention: To add a newline, use \\n instead of \n.
	// ------------------------------------------------------------------------
	
	protected static final String DESC_PARAM_APPLICATION_CONFIGURATION_NAME = 
			"Specifies the name of [https://www.ibm.com/support/knowledgecenter/en/SSCRJU_4.2.0/com.ibm.streams.admin.doc/doc/creating-secure-app-configs.html|application configuration object] "
			+ "that can contain domainId, connectionURL, user, password, iamApiKey, iamTokenEndpoint and filterDocument "
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
	
	protected static final Object PARAMETER_IAM_API_KEY = "iamApiKey";
	
	protected static final Object PARAMETER_IAM_TOKEN_ENDPOINT = "iamTokenEndpoint";
	
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
	private static Logger _trace = Logger.getLogger(AbstractJmxOperator.class.getName());
	
	protected OperatorConfiguration _operatorConfiguration = new OperatorConfiguration();
		
	protected String _domainId = null; // domainId for this PE
	
	/**
	 * The base directory of the application
	 */	
	protected File baseDir = null;
	
	private Metric isConnected;
	private Metric nJMXConnectionAttempts;
	private Metric nBrokenJMXConnections;

    public Metric get_nJMXConnectionAttempts() {
        return this.nJMXConnectionAttempts;
    }
	
    public Metric get_isConnected() {
        return this.isConnected;
    }

    public Metric get_nBrokenJMXConnections() {
        return this.nBrokenJMXConnections;
    }
    
    @CustomMetric(name="nBrokenJMXConnections", kind = Kind.COUNTER, description = "Number of broken JMX connections that have occurred. Notifications may have been lost.")
    public void set_nConnectionLosts(Metric nBrokenJMXConnections) {
        this.nBrokenJMXConnections = nBrokenJMXConnections;
    }
    
    @CustomMetric(name="nJMXConnectionAttempts", kind = Kind.COUNTER, description = "Number of connection attempts to JMX service.")
    public void set_nJMXConnectionAttempts(Metric nConnectionAttempts) {
        this.nJMXConnectionAttempts = nConnectionAttempts;
    }
    
    @CustomMetric(name="isConnected", kind = Kind.GAUGE, description = "Value 1 indicates, that this operator is connected to JMX service. Otherwise value 0 is set, if no connection is established.")
    public void set_isConnected(Metric isConnected) {
        this.isConnected = isConnected;
    }

	@Parameter(
			optional=true,
			description=AbstractJmxOperator.DESC_PARAM_CONNECTION_URL
			)
	public void setConnectionURL(String connectionURL) {
		_operatorConfiguration.set_connectionURL(connectionURL);
	}

	@Parameter(
			optional=true,
			description=AbstractJmxOperator.DESC_PARAM_USER
			)
	public void setUser(String user) {
		_operatorConfiguration.set_user(user);
	}

	@Parameter(
			optional=true,
			description=AbstractJmxOperator.DESC_PARAM_PASSWORD
			)
	public void setPassword(String password) {
		_operatorConfiguration.set_password(password);
	}

	@Parameter(
			optional=true,
			description=AbstractJmxOperator.DESC_PARAM_SSL_OPTION
			)
	public void setSslOption(String sslOption) {
		_operatorConfiguration.set_sslOption(sslOption);
	}
	
	@Parameter(
			optional=true,
			description=AbstractJmxOperator.DESC_PARAM_DOMAIN_ID
			)
	public void setDomainId(String domainId) {
		_operatorConfiguration.set_domainId(domainId);
	}

	@Parameter(
			optional=true,
			description=AbstractJmxOperator.DESC_PARAM_APPLICATION_CONFIGURATION_NAME
			)
	public void setApplicationConfigurationName(String applicationConfigurationName) {
		_operatorConfiguration.set_applicationConfigurationName(applicationConfigurationName);
	}

	@Parameter(optional=true, description = "Specifies IAM API Key. Relevant for IAM authentication case only. If parameter is set, then the parameters user and password are ignored.")
	public void setiamApiKey(String iamApiKey) {
		_operatorConfiguration.set_iamApiKey(iamApiKey);
	}
	
	@Parameter(optional=true, description = "Specifies IAM token endpoint. Relevant for IAM authentication case only. If parameter is not set, then the global endpoint is used: " + OperatorConfiguration.IAM_TOKEN_ENDPOINT)
	public void setiamTokenEndpoint(String iamTokenEndpoint) {
		_operatorConfiguration.set_iamTokenEndpoint(iamTokenEndpoint);
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

		// check required parameters
		if (null == _operatorConfiguration.get_applicationConfigurationName()) {
			if ((null == _operatorConfiguration.get_user()) && (null == _operatorConfiguration.get_password()) && (null == _operatorConfiguration.get_iamApiKey())) {
				throw new com.ibm.streams.operator.DataException("The " + context.getName() + " operator requires parameters 'user' and 'password' or 'applicationConfigurationName' or 'iamApiKey' be applied.");
			}
		}
		else {
			if (context.getPE().isStandalone()) {
				if ((null == _operatorConfiguration.get_user()) && (null == _operatorConfiguration.get_password())) {
					throw new com.ibm.streams.operator.DataException("The " + context.getName() + " operator requires parameters 'user' and 'password' and 'domainId' applied, when running in standalone mode. Application configuration is supported in distributed mode only.");
				}				
			}
		}
		
		this.baseDir = context.getPE().getApplicationDirectory();

		/*
		 * The domainId parameter is optional. If the application developer does
		 * not set it, use the domain id under which the operator itself is
		 * running.
		 */
		if (_operatorConfiguration.get_domainId() == null) {
			if (context.getPE().isStandalone()) {
				throw new com.ibm.streams.operator.DataException("The " + context.getName() + " operator runs in standalone mode and can, therefore, not automatically determine a domain id. The following value must be specified as parameter: domainId");
			}
			String domainId = getApplicationConfigurationDomainId();
			if ("".equals(domainId)) {
				_operatorConfiguration.set_domainId(context.getPE().getDomainId());
				_trace.info("The " + context.getName() + " operator automatically connects to the " + _operatorConfiguration.get_domainId() + " domain.");
			}
			else {
				_operatorConfiguration.set_domainId(domainId);
				_trace.info("The " + context.getName() + " operator connects to the " + _operatorConfiguration.get_domainId() + " domain specified by application configuration.");
			}
		}
		// used to determine if configured domain is the domain where the PE runs
		// if is running in standalone, then the domainId parameter/application configuration is used 
		_domainId = ((context.getPE().isStandalone()) ? _operatorConfiguration.get_domainId() : context.getPE().getDomainId());

		_operatorConfiguration.set_defaultFilterInstance((context.getPE().isStandalone()) ? ".*" : context.getPE().getInstanceId());
		_operatorConfiguration.set_defaultFilterDomain(_domainId);

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
		String iamApiKey = _operatorConfiguration.get_iamApiKey();
		String iamTokenEndpoint = _operatorConfiguration.get_iamTokenEndpoint();
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
			if (properties.containsKey(PARAMETER_IAM_API_KEY)) {
				iamApiKey = properties.get(PARAMETER_IAM_API_KEY);
			}
			if (properties.containsKey(PARAMETER_IAM_TOKEN_ENDPOINT)) {
				iamTokenEndpoint = properties.get(PARAMETER_IAM_TOKEN_ENDPOINT);
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
		if (iamApiKey == null) {
			if (user == null) {
				throw new Exception(MISSING_VALUE + PARAMETER_USER);
			}
			if (password == null) {
				throw new Exception(MISSING_VALUE + PARAMETER_PASSWORD);
			}
		}
		else { 
			// use IAM authentication
			String token = getAccessToken(iamApiKey, iamTokenEndpoint);
			user = "streams-bearer";
			password = "bearertoken:"+token;
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
				get_nJMXConnectionAttempts().increment(); // update metric
				_trace.info("Connect to : " + urls[i]);
				_operatorConfiguration.set_jmxConnector(JMXConnectorFactory.connect(new JMXServiceURL(urls[i]), env));
				get_isConnected().setValue(1);
				break; // exit loop here since a valid connection is established, otherwise exception is thrown.
			} catch (IOException e) {
				_trace.error("connect failed: " + e.getMessage());
				if (i == 0) {
					get_isConnected().setValue(0);
					throw e;
				}
			}
		}
		_operatorConfiguration.set_mbeanServerConnection(_operatorConfiguration.get_jmxConnector().getMBeanServerConnection());
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
	
	private String autoDetectJmxConnect() throws Exception {
		String result = null;
		_trace.debug("_domainId=[" + _domainId + "]");
		_trace.debug("_operatorConfiguration.get_domainId()=[" + _operatorConfiguration.get_domainId() + "]");
		if (_operatorConfiguration.get_domainId().equals(_domainId)) { // running in same domain as configured
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
			// Service URL must start with 
			if (result.startsWith("service:jmx:")) {
				_trace.info("connectionURL=[" + result + "]");
				_operatorConfiguration.set_connectionURL(result);
			}
			else {
				if (result.endsWith(",")) {
					result = result.substring(0, result.length()-1);
				}
				_trace.error("Unable to determine "+PARAMETER_CONNECTION_URL+": " + result);
				throw new Exception("Unable to determine "+PARAMETER_CONNECTION_URL);
			}
		}
		return result;
	}	

	private String getAccessToken(String apiKey, String url) throws Exception {		
		String token = null;
		_trace.debug("get access token with apiKey:" + apiKey);
	
		String[] cmd = { "/bin/sh", "-c", "curl -s -d grant_type=urn:ibm:params:oauth:grant-type:apikey -d apikey="+apiKey+" -H Content-type=application/x-www-form-urlencoded -H Accept=application/json "+url };
			
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
			String cmdResult = output.toString();
			_trace.debug("cmdResult: " + cmdResult);
			JSONArtifact root = JSON.parse(cmdResult);
			JSONObject json = (JSONObject)root;
			Object tokenObj = json.get("access_token");
			token = tokenObj.toString();
			
			_trace.debug("token: " + token);
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (null == token) {
			_trace.error("Unable to determine IAM access token");
			throw new Exception("Unable to determine IAM access token");			
		}		
		return token;
	}

	private String autoDetectJmxSslOption(String user, String password) {		
		String sslOption = null;
		if (_operatorConfiguration.get_domainId().equals(_domainId)) { // running in same domain as configured
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
