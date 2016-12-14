package com.ibm.streamsx.metrics.internal;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.ibm.streams.management.Metric;

/**
 * Operators and input and output ports own metrics. By deriving from this
 * class, the corresponding handler classes get some useful utilities.
 */
abstract class MetricOwningHandler {

	/**
	 * Modes to specify how metrics are registered for capturing:
	 * 
	 * * {@code InitialMetricsRegistration}:
	 *   <p>
	 *   As soon as the parent MxBean is available, also the metrics are
	 *   available. Decide once during the setup of the parent, whether
	 *   metrics shall be captured.
	 *   </p>
	 *   
	 * * {@code DynamicMetricsRegistration}:
	 *   <p>
	 *   The metrics are either not available as soon as the parent MxBean
	 *   is available, or the metrics set can change like for custom metrics
	 *   that are created in a state, onTuple, or onPunct clause.
	 *   </p><p>
	 *   The process to decide whether metrics shall be captured or ignored,
	 *   has to run during each capturing cycle.
	 *   </p>
	 */
	enum MetricsRegistrationMode {
		InitialMetricsRegistration,
		DynamicMetricsRegistration
	};
	
	/**
	 * The operator-global configuration data and globally used objects.
	 */
	protected OperatorConfiguration _operatorConfiguration = null;

	/**
	 * Specifies whether the metrics are registered immediately as soon as the
	 * JMX operator or port MxBean is accessible (InitialMetricsRegistration),
	 * or whether the set of metrics can change during runtime, like for
	 * custom metrics, requiring a DynamicMetricsRegistration.
	 * 
	 * @see MetricsRegistrationMode
	 */
	private MetricsRegistrationMode _metricsRegistrationMode = MetricsRegistrationMode.InitialMetricsRegistration;

	/**
	 * This map holds all metrics that are captured.
	 */
	private Map<String /* metric name */, IMetricEvaluator> _capturedMetrics = new HashMap<String, IMetricEvaluator>();
	
	/**
	 * This set holds, in DynamicMetricsRegistration mode, all metric names 
	 * from metrics that are ignored. Using this set improves performance
	 * because we do not need to match the name against the configured filters
	 * each time. 
	 */
	private Set<String /* metric name */> _ignoredMetrics = new HashSet<String>();

	/**
	 * 
	 * @param metricsRegistrationMode
	 * Specify whether registering metrics for the capturing process run once
	 * or during every capturing cycle.
	 */
	protected MetricOwningHandler(MetricsRegistrationMode metricsRegistrationMode) {
		_metricsRegistrationMode = metricsRegistrationMode;
	}
	
	/**
	 * Set the metric-relevant tuple attributes and submit the tuple.
	 * All other domain-, instance-, job-, operator-, port-relevant
	 * tuple attributes are already set.
	 * 
	 * @param metric
	 * The JMX metric object that holds the metric-relevant information.
	 * 
	 * @throws Exception
	 * The exception is thrown if submitting the tuple fails.
	 */
	protected void submitMetric(Metric metric) throws Exception {
		_operatorConfiguration.get_tupleContainer().setMetricType(metric.getMetricType());
		_operatorConfiguration.get_tupleContainer().setMetricKind(metric.getMetricKind());
		_operatorConfiguration.get_tupleContainer().setMetricName(metric.getName());
		_operatorConfiguration.get_tupleContainer().setMetricValue(metric.getValue());
		_operatorConfiguration.get_tupleContainer().setLastTimeRetrieved(metric.getLastTimeRetrieved());
		_operatorConfiguration.get_tupleContainer().submit();
	}

	/**
	 * Determine whether a given metric name is relevant, which means, the
	 * corresponding metric name matches the specified filters and shall be
	 * captured.
	 * 
	 * @param metricName
	 * Specifies the metric name that is evaluated.
	 * 
	 * @return
	 * True if the metric is relevant and shall be captured, else false.
	 */
	protected abstract boolean isRelevantMetric(String metricName);
	
	/**
	 * Retrieve the metrics using the metrics' parent object, for example,
	 * the OperatorMXBean, OperatorInputPortMXBean, or OperatorOutputPortMXBean.
	 *  
	 * @return
	 * The retrieved metrics.
	 */
	protected abstract Set<Metric> retrieveMetrics();
	
	/**
	 * In case of InitialMetricsRegistration mode, retrieve the metrics while
	 * constructing a class object and register all relevant metrics.
	 */
	protected void registerMetrics() {
		if (_metricsRegistrationMode.equals(MetricsRegistrationMode.InitialMetricsRegistration)) {
			Set<Metric> metrics = retrieveMetrics();
			for(Metric metric: metrics) {
				String metricName = metric.getName();
				if (isRelevantMetric(metricName)) {
					_capturedMetrics.put(metricName, _operatorConfiguration.newDefaultMetricEvaluator());
				}
			}
		}
	}
	
	/**
	 * Retrieve metrics, depending on the registration mode evaluate which
	 * metrics are relevant, and submit tuples for changed metric values.
	 * 
	 * @throws Exception
	 * Throws Exception if submitting the tuple fails. 
	 */
	protected void captureAndSubmitChangedMetrics() throws Exception {
		if (_metricsRegistrationMode.equals(MetricsRegistrationMode.InitialMetricsRegistration)) {
			/*
			 * Registration happened once while setting up the parent object.
			 * If there are no relevant metrics, return immediately.
			 */
			if(_capturedMetrics.size() > 0) {
				Set<Metric> metrics = retrieveMetrics();
				for (Metric metric : metrics) {
					String metricName = metric.getName();
					if (_capturedMetrics.containsKey(metricName)) {
						IMetricEvaluator evaluator = _capturedMetrics.get(metricName);
						if (evaluator.isSubmittable(metric)) {
							evaluator.updateStatus(metric);
							submitMetric(metric);
						}
					}
				}
			}
		}
		else {
			/*
			 * The metrics must always be retrieved because we must decide
			 * periodically which metrics are relevant. 
			 */
			Set<Metric> metrics = retrieveMetrics();
			/*
			 * This solution is required for metrics that do not exist as soon
			 * as the parent object exists, for example:
			 * 
			 * The custom metrics can be created in an operator's state, onTuple,
			 * or onPunct clause. Therefore, it is not ensured that the custom
			 * metric is available as soon as the operator is available.
			 * 
			 * The OperatorContextMXBean supports a CUSTOM_METRIC_CREATED
			 * notification, but this notification is not available via the
			 * JMX API. It seems as if it is available only within an operator,
			 * for this specific operator.
			 * 
			 * The implemented solution is not optimal:
			 * 
			 * Always retrieve the custom metrics. If a custom metric was not
			 * handled before, verify whether its name matches the filters.
			 * If it matches the filters, store the metric in the
			 * _capturedCustomMetrics map. If it does not match, store the name
			 * in the _uncapturedCustomMetrics set. If a custom metric was handled
			 * before, check whether it is either in the _capturedCustomMetrics map
			 * or the _uncapturedCustomMetrics set, and act accordingly.
			 */
			for (Metric metric : metrics) {
				String metricName = metric.getName();
				/*
				 * Metric shall be captured.
				 */
				if (_capturedMetrics.containsKey(metricName)) {
					IMetricEvaluator evaluator = _capturedMetrics.get(metricName);
					if (evaluator.isSubmittable(metric)) {
						evaluator.updateStatus(metric);
						submitMetric(metric);
					}
				}
				/*
				 * Metric shall be ignored.
				 */
				else if (_ignoredMetrics.contains(metricName)) {
					// Ignore this metric because it does not match the filters.
				}
				/*
				 * Decide whether the metric shall be captured or ignored.
				 */
				else if(isRelevantMetric(metricName)) {
					IMetricEvaluator evaluator = _operatorConfiguration.newDefaultMetricEvaluator();
					_capturedMetrics.put(metricName, evaluator);
					if (evaluator.isSubmittable(metric)) {
						evaluator.updateStatus(metric);
						submitMetric(metric);
					}
				}
				else {
					_ignoredMetrics.add(metricName);
				}
			}
		}
	}
	
}
