package com.quickserverlab.quickcached.client;

public class MemcachedException extends Exception{
	
	public MemcachedException() {
		super();
	}
	
	public MemcachedException(String name) {
		super(name);
	}
	
}
