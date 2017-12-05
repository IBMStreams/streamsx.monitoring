//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.metrics.internal.monitor;


import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ibm.json.java.JSON;
import com.ibm.json.java.JSONArray;
import com.ibm.json.java.JSONArtifact;
import com.ibm.json.java.JSONObject;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.ProcessingElement;
import com.ibm.streams.operator.Tuple;

/**
 * This class parses and stores the thresholds objects within a thresholds JSON 
 * document. The document can originate from a system JSON file or an application 
 * configuration property.
 */
public class ThresholdDocument {
	
	/**
	 * Threshold document's application configuration property name.
	 */
	private static final String PARAMETER_THRESHOLD_DOCUMENT = "thresholdDocument";
	
	/**
	 * Missing value error message.
	 */
	private static final String MISSING_VALUE = "The following value must be specified as parameter or in the application configuration: ";
	
	/**
	 * Stringified tab regular expression.
	 */
	private static final String STRINGIFIED_TAB_REGEX  = "\\\\t";
	
	// ------------------------------------------------------------------------
	// Implementation.
	// ------------------------------------------------------------------------
	
	/**
	 * Extracted threshold objects from document.
	 */
	private List<ThresholdRule> thresholdRules = new ArrayList<ThresholdRule>();
	
	/**
	 * Local threshold file object.
	 */
	File thresholdFile;
	
	/**
	 * Application configuration name and active document. Active document is compared against 
	 * current one to determine when document has been modified.
	 */
	private String applicationConfigurationName;
	private String activeThresholdDocumentFromApplicationConfiguration;
	
	/**
	 * Operator context (used to get application configuration).
	 */
	OperatorContext context;
	
	private File baseDir = null;
	String thresholdDocumentPath = null;
	
	/**
	 * Time thresholdDocument file was last modified (used to check for modifications).
	 */
	long lastModified;
	
	/**
	 * Time thresholdDocument was parsed (used to determine whether threshold timeframes have elapsed).
	 */
	protected Long parsedTime;
	
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
	 * Set thresholdDocument file from given file path.
	 */
	public void setFileThresholdDocument(String thresholdDocumentPath) throws IOException {
		if (!("".equals(thresholdDocumentPath))) {
			this.thresholdDocumentPath = thresholdDocumentPath;
		}
	}
	
	/**
	 * Set application configuration properties.
	 */
	public void setApplicationConfigurationName(String applicationConfigurationName) {
		if (!("".equals(applicationConfigurationName))) {
			this.applicationConfigurationName = applicationConfigurationName;
		}
	}
	
	/**
	 * Set OperatorContext. Used for getting application configuration properties 
	 * and submitting tuples.
	 */
	public void setOperatorContext(OperatorContext context) {
		this.context = context;
		this.baseDir = context.getPE().getApplicationDirectory();
	}
    
	/**
	 * Check if thresholdDocument file has been modified.
	 * 
	 * @return
	 * Returns true if thresholdDocument file has been modified.
	 * 
	 * @throws IOException
	 * Throws IOException if file is not found.
	 */
    private Boolean thresholdFileChanged() throws IOException {
    	if (thresholdFile != null && parsedTime == null) {
    		return true;
    	} else if (thresholdFile != null && lastModified < thresholdFile.lastModified()) {
			lastModified = thresholdFile.lastModified();
			return true;
		} else if (thresholdFile == null){
			// Should never get to this point.
			throw new IOException("Expected threshold document file to be defined.");
		}
		
		return false;
    }
    
	/**
	 * Check if application configuration's thresholdDocument has been modified.
	 * 
	 * @return
	 * Returns true if thresholdDocument file has been modified.
	 */
    private Boolean applicationConfigurationChanged() {
    	String thresholdDocument = getApplicationConfiguration(applicationConfigurationName)
    										.get(PARAMETER_THRESHOLD_DOCUMENT);

		if (activeThresholdDocumentFromApplicationConfiguration == null || 
				!activeThresholdDocumentFromApplicationConfiguration.equals(thresholdDocument)) {
			return true;
		}
		
		return false;
    }
    
    /**
     * Check application configuration's thresholdDocument property for modifications. If that doesn't 
     * exist, check thresholdDocument file for modifications.
     * 
     * If modification is detected, update parsed thresholds.
     * @throws Exception 
     */
    public void detectAndProcessModifiedThresholds() throws Exception {
		if (applicationConfigurationName != null) {
			if (applicationConfigurationChanged()) {
				setupApplicationConfigurationThresholds();
				resetAlerts();
			}
		} else if (thresholdDocumentPath != null) {
			if (null == thresholdFile) {
				thresholdFile = new File(makeAbsolute(this.baseDir, thresholdDocumentPath));
				lastModified = thresholdFile.lastModified();
			}
			if (thresholdFileChanged()) {
				setupFileThresholds();
				resetAlerts();
			}
		} else {
			throw new IOException(MISSING_VALUE + PARAMETER_THRESHOLD_DOCUMENT);
		}
    }
	
	/**
	 * Setup the file's thresholds.
	 * @throws Exception 
	 */
	private void setupFileThresholds() throws Exception {
		try (InputStream inputStream = Files.newInputStream(thresholdFile.toPath())) {
			parseThresholdDocument(inputStream);
		} catch (IOException e) {
			throw new IOException("File thresholdDocument parse error: " + e);
		}
	}
	
    /**
	 * Setup the application configuration's thresholds.
     * @throws Exception 
	 */
	private void setupApplicationConfigurationThresholds() throws Exception {
		Map<String,String> properties = getApplicationConfiguration(applicationConfigurationName);
		
		if (properties.containsKey(PARAMETER_THRESHOLD_DOCUMENT)) {
			String thresholdDocument = properties.get(PARAMETER_THRESHOLD_DOCUMENT);
			activeThresholdDocumentFromApplicationConfiguration = thresholdDocument;
			
			// Remove tabs.
			String thresholdDoc = thresholdDocument.replaceAll(STRINGIFIED_TAB_REGEX, "");
			
			try(InputStream inputStream = new ByteArrayInputStream(thresholdDoc.getBytes())) {
				parseThresholdDocument(inputStream);
			} catch (IOException e) {
				throw new IOException("Application configuration thresholdDocument parse error: " + e);
			}
		}
	}
	
	/**
	 * Parse thresholdDocument JSON for threshold rules.
	 * @throws Exception 
	 */
	private void parseThresholdDocument(InputStream inputStream) throws Exception {
		
		// Clear any existing threshold rules.
		thresholdRules = new ArrayList<ThresholdRule>();
		
		// Construct and add threshold rules.
		try {
			JSONArtifact root = JSON.parse(inputStream);
			if (root instanceof JSONArray) {
				for (Object object : (JSONArray)root) {
					if (object instanceof JSONObject) {
						thresholdRules.add(new ThresholdRule((JSONObject)object));
					}
				}
			} else if (root instanceof JSONObject) {
				thresholdRules.add(new ThresholdRule((JSONObject)root));
			}
			
			// Store parse time.
			parsedTime = System.currentTimeMillis();
			
		} catch (NullPointerException|IOException e) {
			throw new Exception("JSON parsing error, please try modifying thresholds JSON document: " + e);
		} finally {
			inputStream.close();
		}
	}
	
	
	/**
	 * Check whether the thresholdDocument is monitoring the given metric. 
	 * Used to check whether or not to store incoming metrics.
	 * 
	 * @return
	 * Returns true if the given threshold exists.
	 */
	public Boolean monitoringMetric(String metricName) {
		for (ThresholdRule thresholdRule : thresholdRules) {
			if (thresholdRule.getMetricName().equals(metricName)) {
				return true;
			}
		}
		
		return false;
	}
	
	/**
	 * Get the threshold alerts. Alerts are generated when/if stored metrics violate 
	 * any stored threshold rules.
	 */
	public void checkAndSubmitAlerts(StoredTuples storedMetricTuples) {
		
		// Get matching stored metrics and thresholds.
		if (storedMetricTuples.getMonitoredMetric() != null) {
			String monitoredMetricName = storedMetricTuples.getMonitoredMetric().getString(Metrics.METRIC_NAME);
			
	    	List<ThresholdRule> matchingThresholdRules = getMatchingThresholdRules(monitoredMetricName);
	    	List<Tuple> matchingStoredMetrics = storedMetricTuples.getMatchingTuples(monitoredMetricName);
	    	
	    	// Check and submit alerts.
	    	for (ThresholdRule thresholdRule : matchingThresholdRules) {
	    		thresholdRule.checkAndSubmitAlerts(matchingStoredMetrics, parsedTime, context);
	    	}
		}
	}
	
	/**
	 * Get ThresholdRules that match the given metric name.
	 * 
	 * @return
	 * Returns the matching ThresholdRules.
	 */
	private List<ThresholdRule> getMatchingThresholdRules(String metricName) {
		List<ThresholdRule> matchingThresholdRules = new ArrayList<ThresholdRule>();
		
		for (ThresholdRule thresholdRule : thresholdRules) {
			if (thresholdRule.getMetricName().equals(metricName)) {
				matchingThresholdRules.add(thresholdRule);
			}
		}
		
		return matchingThresholdRules;
	}
	
	/**
	 * Reset all threshold alerts.
	 */
	private void resetAlerts() {
		for (ThresholdRule thresholdRule : thresholdRules) {
			thresholdRule.resetAlerts();;
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
	private Map<String,String> getApplicationConfiguration(String applicationConfigurationName) {
		Map<String,String> properties = null;
		
		try {
			ProcessingElement pe = context.getPE();
			Method method = ProcessingElement.class.getMethod("getApplicationConfiguration", new Class[]{String.class});
			Object returnedObject = method.invoke(pe, applicationConfigurationName);
			properties = (Map<String,String>)returnedObject;
			
		} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			properties = new HashMap<>();
		}
		
		return properties;
	}
}
