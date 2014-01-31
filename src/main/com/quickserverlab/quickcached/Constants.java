package com.quickserverlab.quickcached;

import java.text.DecimalFormat;


/**
 * <p> For constant values </p>
 * @author Ravindra HV
 */
public class Constants {
	
	public static final String EMPTY_STRING = "";
	
	public static final String CONFIG_KEY__SLOW_RESPONSE_AVG_RANGE = "SLOW_RESPONSE_AVG_RANGE";
	
	/**
	 * Specifies the number of previous 'n' requests which will be considered in calculating the the average slow response.
	 * @see #CONFIG_KEY__SLOW_RESPONSE_AVG_RANGE
	 */
	public static final int DEFAULT_CONFIG_VALUE__SLOW_RESPONSE_AVG_RANGE = 100;
	
	public static final String DEFAULT_DECIMAL_PATTERN = "#0.00";
	public static final DecimalFormat DEFAULT_DECIMAL_FORMAT = new DecimalFormat(DEFAULT_DECIMAL_PATTERN);
	public static final String NOT_APPLICABLE__ABBR = "N/A";
	public static final String STATS_KEY__SLOW_RES_AVG_PERCENT = "slow_res_avg_percent";
	public static final int DEFAULT_INT_UN_INITIALIZED_VALUE = -1;

}
