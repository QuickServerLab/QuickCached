package com.quickserverlab.quickcached.client;

import java.io.IOException;

/**
 *
 * @author akshath
 */
public class TimeoutException extends IOException {
	public TimeoutException() {
		super();
	}
	
	public TimeoutException(String name) {
		super(name);
	}
}
