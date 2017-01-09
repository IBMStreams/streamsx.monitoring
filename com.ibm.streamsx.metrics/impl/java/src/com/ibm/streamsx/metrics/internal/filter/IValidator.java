package com.ibm.streamsx.metrics.internal.filter;

interface IValidator {

	boolean validate(String key, Object object);

}
