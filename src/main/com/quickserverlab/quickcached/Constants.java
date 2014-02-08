package com.quickserverlab.quickcached;

import java.text.DecimalFormat;


/**
 * <p> For constant values </p>
 * @author Ravindra HV
 * @author Akshath
 */
public class Constants {
	public static final String EMPTY_STRING = "";
		
	public static final String DEFAULT_DECIMAL_PATTERN = "#0.0000";
	
	public static final DecimalFormat DEFAULT_DECIMAL_FORMAT = new DecimalFormat(DEFAULT_DECIMAL_PATTERN);
	
	public static final String NOT_APPLICABLE_ABBR = "N/A";
	public static final int DEFAULT_INT_UN_INITIALIZED_VALUE = -1;
}
