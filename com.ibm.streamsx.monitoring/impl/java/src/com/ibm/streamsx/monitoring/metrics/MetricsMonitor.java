//
// ****************************************************************************
// * Copyright (C) 2017, International Business Machines Corporation          *
// * All rights reserved.                                                     *
// ****************************************************************************
//

package com.ibm.streamsx.monitoring.metrics;

import java.io.IOException;
import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streamsx.monitoring.metrics.internal.monitor.Metrics;
import com.ibm.streamsx.monitoring.metrics.internal.monitor.StoredTuples;
import com.ibm.streamsx.monitoring.metrics.internal.monitor.ThresholdDocument;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.Parameter;
import com.ibm.streams.operator.model.PrimitiveOperator;

@PrimitiveOperator(
		name="MetricsMonitor",
		namespace="com.ibm.streamsx.monitoring.metrics",
		description=MetricsMonitor.DESC_OPERATOR
		)
@InputPorts({
	@InputPortSet(
			description="Port that ingests tuples",
			cardinality=1, optional=false,
			windowingMode=WindowMode.NonWindowed,
			windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)}
		)
@OutputPorts({
	@OutputPortSet(
			cardinality=1,
			optional=false,
			windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating,
			description="Specifies the output port."
			)
})
public class MetricsMonitor extends AbstractOperator {

	// ------------------------------------------------------------------------
	// Documentation.
	// Attention: To add a newline, use \\n instead of \n.
	// ------------------------------------------------------------------------
	
	static final String DESC_OPERATOR = 
			"The MetricsMonitor operator connects to the MetricsSource operator "
			+ "and receives incoming metric tuples. It compares the metric values "
			+ "against threshold rules that the user defines. As a result, if any "
			+ "threshold rules are violated, alert tuples are outputted with "
			+ "details about the violated threshold rule, the current metric value, "
			+ "and additional information about the retrieved metric.\\n"
			+ "\\n"
			+ "As an application developer, you provide the so-called threshold "
			+ "document. The threshold document accepts a list of threshold rules "
			+ "that are evaluated at a set interval (defined by the scanPeriod "
			+ "parameter from the MetricsSource operator).\\n"
			+ "\\n"
			+ "The threshold document is, by default, empty. In other words, "
			+ "nothing is being monitored.\\n"
			+ "\\n"
			+ "+ Threshold document\\n"
			+ "\\n"
			+ "The threshold document contains a list of threshold rules. Each "
			+ "threshold rule is defined as a JSON object. This JSON object "
			+ "contains 3 properties:\\n"
			+ "\\n"
			+ "**metricName**: String specifying which metric to monitor.\\n"
			+ "\\n"
			+ "**thresholds (optional)**: JSON object specifying 1 or more "
			+ "thresholds (example format below). There are 4 supported threshold "
			+ "types the user can select from:\\n"
			+ "\\n"
			+ "* **value**: A value to evaluate incoming metric values against.\\n"
			+ "\\n"
			+ "* **rate**: The rate of incoming metric values, given a timeframe.\\n"
			+ "\\n"
			+ "* **rollingAverage**: The rolling average of incoming metric values, "
			+ "given a timeframe.\\n"
			+ "\\n"
			+ "* **increasePercentage**: The percentage that incoming metric values "
			+ "are increasing/decreasing, given a timeframe.\\n"
			+ "\\n"
			+ "Note that you may choose to add an operator (\\\">\\\", \\\">=\\\", "
			+ "\\\"<\\\", or \\\"<=\\\") in front of the threshold value to "
			+ "indicate whether you want to monitor for values above or below "
			+ "a certain threshold.\\n"
			+ "\\n"
			+ "**filters (optional)**: JSON object specifying 1 or more filters. "
			+ "Filters give users the option to further filter which metrics they "
			+ "want monitored. Simply provide an attribute of the incoming metric "
			+ "tuples as a key, and a regular expression as the value (example "
			+ "format below).\\n"
			+ "\\n"
			+ "The following threshold document monitors the **com.ibm.streamsx."
			+ "monitoring.sample.MetricsSource.MemoryMetrics** sample application "
			+ "for when its nResidentMemoryConsumption goes above 10000KB:\\n"
			+ "\\n"			
			+ "    [\\n"
			+ "      {\\n"
			+ "        \\\"metricname\\\":\\\"nResidentMemoryConsumption\\\",\\n"
			+ "        \\\"thresholds\\\":\\n"
			+ "        {\\n"
			+ "          \\\"value\\\":\\\">10000\\\",\\n"
			+ "        },\\n"
			+ "        \\\"filters\\\":\\n"
			+ "        {\\n"
			+ "          \\\"jobName\\\":\\\"com.ibm.streamsx.sample.MetricsSource"
			+ ".MemoryMetrics.*\\\"\\n"
			+ "        }\\n"
			+ "    ]\\n"
			+ "\\n"
			+ "The following threshold document monitors the **com.ibm.streamsx."
			+ "monitoring.sample.MetricsMonitor.HighCongestion** sample application "
			+ "for when its congestionFactor has a rolling average greater than 80 "
			+ "over a timeframe of 20s:\\n"
			+ "\\n"			
			+ "    [\\n"
			+ "      {\\n"
			+ "        \\\"metricname\\\":\\\"congestionFactor\\\",\\n"
			+ "        \\\"thresholds\\\":\\n"
			+ "        {\\n"
			+ "          \\\"value\\\":\\\">=80,20000\\\",\\n"
			+ "        },\\n"
			+ "        \\\"filters\\\":\\n"
			+ "        {\\n"
			+ "          \\\"jobName\\\":\\\"com.ibm.streamsx.sample.MetricsMonitor"
			+ ".HighCongestion.*\\\"\\n"
			+ "        }\\n"
			+ "    ]\\n"
			+ "\\n"
			;
	
	// ------------------------------------------------------------------------
	// Operator Parameters
	// ------------------------------------------------------------------------
	
	@Parameter(
			optional=true,
			description="Path to thresholdDocument JSON file."
			)
	public void setThresholdDocumentPath(String thresholdDocumentPath) throws IOException {
		thresholdDocument.setFileThresholdDocument(thresholdDocumentPath);
	}
	
	@Parameter(
			optional=true,
			description="Name of application configuration containing thresholdDocument JSON."
			)
	public void setApplicationConfigurationName(String applicationConfigurationName) throws IOException {
		thresholdDocument.setApplicationConfigurationName(applicationConfigurationName);
	}
	
	@Parameter(
			optional=true,
			description="Maximum number of tuples to query for each metric defined in the thresholdDocument."
			)
	public void setMaxTuplesToQuery(int maxTuplesToQuery) {
		this.maxTuplesToQuery = maxTuplesToQuery;
	}
	
	// ------------------------------------------------------------------------
	// Implementation
	// ------------------------------------------------------------------------
	
	/**
	 * Thresholds container. Contains the parsed thresholds and functions for reading and
	 * checking whether thresholds have been reached.
	 */
	private ThresholdDocument thresholdDocument = new ThresholdDocument();
	
	/**
	 * Store tuple data for calculating rate, rolling average, and increase percentage, later. 
	 * These values are compared against the thresholdDocument's thresholds.
	 */
	private StoredTuples storedTuples = new StoredTuples();
	
	/**
	 * Optional operator parameter. Specify maximum number of tuples to query for each metric 
	 * specified in thresholdDocument.
	 */
	private int maxTuplesToQuery = -1;
	
	/**
	 * Logger for tracing.
	 */
	private static Logger _trace = Logger.getLogger(MetricsMonitor.class.getName());

	/**
	 * Initialize this operator. Called once before any tuples are processed.
	 * @param context OperatorContext for this operator.
	 * @throws Exception Operator failure, will cause the enclosing PE to terminate.
	 */
	@Override
	public synchronized void initialize(OperatorContext context) throws Exception {
		
		// Must call super.initialize(context) to correctly setup an operator.
		super.initialize(context);
        _trace.trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() 
        		+ " in Job: " + context.getPE().getJobId());
        
        thresholdDocument.setOperatorContext(context);
	}

	/**
	 * Store incoming metric tuple. Check the stored data against defined thresholds. Send out 
	 * alerts for any threshold violations found.
	 * 
	 * @throws
	 * Throws an IOException if thresholdDocument is neither defined in operator parameters 
	 * (file path) nor in application configuration (JSON string).
	 */
    @Override
    public void process(StreamingInput<Tuple> stream, Tuple tuple) {
    	Boolean isMetricTuple = stream.getStreamSchema().getAttributeNames().contains(Metrics.METRIC_NAME);
    	
    	if (isMetricTuple) {
        	storeTuple(stream, tuple);
    	}
    }
    
    /**
     * Store incoming Tuples.
     */
    private void storeTuple(StreamingInput<Tuple> stream, Tuple tuple) {
    	
    	// Get and store metric Tuple.
    	try {
        	String metricName = tuple.getString(Metrics.METRIC_NAME);
        	
        	if (thresholdDocument != null && thresholdDocument.monitoringMetric(metricName)) {
	        	storedTuples.add(tuple);
	        	cleanStoredMetrics(metricName);
			}
    	} catch (Exception e) {
    		_trace.error("Tuple parse error: " + e);
    		return;
    	}
    }
    
    /**
     * Clear stored metrics Tuples that are unused. The amount of Tuples to clear is defined 
     * by the maxTuplesToQuery parameter.
     */
    private void cleanStoredMetrics(String metricName) {
    	if (maxTuplesToQuery != -1) {
	    	int numExtraElements = storedTuples.getSize(metricName) - maxTuplesToQuery;
	    	storedTuples.remove(metricName, numExtraElements);
    	}
    }
    
    /**
     * MetricsSource outputs a window marker on every scanPeriod interval. When this 
     * is outputted, do 3 things:
     * 
     * 1. Check for any threshold modifications.
     * 2. Check if any threshold rules are violated.
     * 3. If any are, submit an alert.
     * 
     * @param stream Port the punctuation is arriving on.
     * @param mark The punctuation mark
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public void processPunctuation(StreamingInput<Tuple> stream, Punctuation mark) throws Exception {
    	
    	if (mark.equals(Punctuation.WINDOW_MARKER)) {
    		
	    	 // Check for changes in thresholdDocument.
	    	try {
	    		thresholdDocument.detectAndProcessModifiedThresholds();
	    	} catch (IOException e) {
	    		_trace.error(e);
	    	}
    	
	    	// Check and submit alerts.
	    	thresholdDocument.checkAndSubmitAlerts(storedTuples);
    	}
    }
    
    /**
     * Shutdown this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    public synchronized void shutdown() throws Exception {
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );

        // Must call super.shutdown()
        super.shutdown();
    }
}
