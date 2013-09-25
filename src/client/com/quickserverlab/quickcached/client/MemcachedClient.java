package com.quickserverlab.quickcached.client;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author Akshathkumar Shetty
 */
public abstract class MemcachedClient {
	public static final String SpyMemcachedImpl = "SpyMemcached";
	public static final String XMemcachedImpl = "XMemcached";
	public static final String QuickCachedImpl = "QuickCached";
	
	private static String defaultImpl = XMemcachedImpl;	
	
	private static Map implMap = new HashMap();
	public static void registerImpl(String implName, String fullClassName) {
		implMap.put(implName, fullClassName);
	}
	
	static {
		registerImpl(SpyMemcachedImpl, "com.quickserverlab.quickcached.client.impl.SpyMemcachedImpl");
		registerImpl(XMemcachedImpl, "com.quickserverlab.quickcached.client.impl.XMemcachedImpl");
		registerImpl(QuickCachedImpl, "com.quickserverlab.quickcached.client.impl.QuickCachedClientImpl");
		
		String impl = System.getProperty("com.quickserverlab.quickcached.client.defaultImpl");
		if(impl!=null) {
			defaultImpl = impl;
		}
	}
	
	public static MemcachedClient getInstance() 
			throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		return getInstance(null);
	}
	
	public static MemcachedClient getInstance(String implName) 
			throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		String fullClassName = (String) implMap.get(implName);
		
		if(fullClassName==null) {
			fullClassName = (String) implMap.get(defaultImpl);
		}
		
		MemcachedClient client = (MemcachedClient) Class.forName(fullClassName).newInstance();
		
		String binaryConnection = System.getProperty("com.quickserverlab.quickcached.client.binaryConnection");
		if(binaryConnection==null) {
			client.setUseBinaryConnection(false);
		} else if(binaryConnection.equalsIgnoreCase("true")) {
			client.setUseBinaryConnection(true);
		} else {
			client.setUseBinaryConnection(false);
		}
		return client;
	}

	private long defaultTimeoutMiliSec = 1000;//1sec
	public long getDefaultTimeoutMiliSec() {
		return defaultTimeoutMiliSec;
	}

	public void setDefaultTimeoutMiliSec(int aDefaultTimeoutMiliSec) {
		defaultTimeoutMiliSec = aDefaultTimeoutMiliSec;
	}
	
	public abstract void setUseBinaryConnection(boolean flag);
	public abstract void setConnectionPoolSize(int size);

	public abstract void setAddresses(String list);
	public abstract void init() throws IOException;
	public abstract void stop() throws IOException ;
	
	public abstract void addServer(String list) throws IOException;
	public abstract void removeServer(String list);
	
	public abstract void set(String key, int ttlSec, Object value, long timeoutMiliSec) 
			throws TimeoutException;       
	public abstract Object get(String key, long timeoutMiliSec) throws MemcachedException, TimeoutException;
	
	public abstract CASValue gets(String key, long timeoutMiliSec) 
			throws TimeoutException, MemcachedException;
	
	public abstract CASResponse cas(String key, Object value, int ttlSec, long cas, long timeoutMiliSec) 
			throws TimeoutException, MemcachedException;
        
	public abstract <T> java.util.Map<java.lang.String,T> getBulk(Collection<String> keyCollection, 
		long timeoutMiliSec) 
			throws TimeoutException, MemcachedException;
        
	public abstract boolean delete(String key, long timeoutMiliSec) throws TimeoutException;
	public abstract void flushAll() throws TimeoutException;
	
	public abstract boolean touch(String key, int ttlSec, long timeoutMiliSec) throws TimeoutException;
	public abstract Object gat(String key, int ttlSec, long timeoutMiliSec) throws TimeoutException;
        
	public abstract Map getStats() throws Exception;
	public abstract Object getBaseClient();
	
	public abstract boolean add(String key, int ttlSec, Object value, long timeoutMiliSec) 
			throws TimeoutException;
	public abstract boolean replace(String key, int ttlSec, Object value, long timeoutMiliSec) 
			throws TimeoutException;
	public abstract boolean append(String key, Object value, long timeoutMiliSec) 
			throws TimeoutException;
	public abstract boolean prepend(String key, Object value, long timeoutMiliSec) 
			throws TimeoutException;

	public abstract long increment(String key, int delta, long timeoutMiliSec) 
			throws TimeoutException, MemcachedException;
	public abstract long increment(String key, int delta, long defaultValue, long timeoutMiliSec) 
			throws TimeoutException, MemcachedException;
	public abstract long increment(String key, int delta, long defaultValue, int ttlSec, long timeoutMiliSec) 
			throws TimeoutException, MemcachedException;
	public abstract void incrementWithNoReply(String key, int delta)
			throws MemcachedException;
        
	public abstract long decrement(String key, int delta, long timeoutMiliSec) 
			throws TimeoutException, MemcachedException;
	public abstract long decrement(String key, int delta, long defaultValue, long timeoutMiliSec) 
			throws TimeoutException, MemcachedException;
	public abstract long decrement(String key, int delta, long defaultValue, int ttlSec, long timeoutMiliSec) 
			throws TimeoutException, MemcachedException;
	public abstract void decrementWithNoReply(String key, int delta)
			throws MemcachedException;
	
	public abstract Map getVersions() throws TimeoutException;	
	
	public void set(String key, int ttlSec, Object value) 
			throws TimeoutException {
		set(key, ttlSec, value, defaultTimeoutMiliSec);
	}
	public Object get(String key) throws MemcachedException, TimeoutException {
		return get(key, defaultTimeoutMiliSec);
	}

	public CASValue gets(String key) throws TimeoutException, MemcachedException {
		return gets(key, defaultTimeoutMiliSec);
	}

	public CASResponse cas(String key, Object value, int ttlSec, long cas) 
			throws TimeoutException, MemcachedException {
		return cas(key, value, ttlSec, cas, defaultTimeoutMiliSec);
	}

	public Map getBulk(Collection<String> keyCollections) 
			throws TimeoutException, com.quickserverlab.quickcached.client.MemcachedException {
		return getBulk(keyCollections, defaultTimeoutMiliSec);
	}        

	public boolean delete(String key) throws TimeoutException {
		return delete(key, defaultTimeoutMiliSec);
	}
	
	public boolean add(String key, int ttlSec, Object value) throws TimeoutException {
		return add(key, ttlSec, value, defaultTimeoutMiliSec);
	}
	public boolean replace(String key, int ttlSec, Object value) 
			throws TimeoutException {
		return replace(key, ttlSec, value, defaultTimeoutMiliSec);
	}
	public boolean append(String key, Object value) 
			throws TimeoutException {
		return append(key, value, defaultTimeoutMiliSec);
	}
	public boolean prepend(String key, Object value) 
			throws TimeoutException {
		return prepend(key, value, defaultTimeoutMiliSec);
	}
        
    public long increment(String key, int delta, int defaultValue) 
			throws TimeoutException, MemcachedException{
		return increment(key, delta, defaultValue, defaultTimeoutMiliSec);
    }
    public long increment(String key, int delta) 
			throws TimeoutException, MemcachedException{
		return increment(key, delta, defaultTimeoutMiliSec);
    }	
    public long increment(String key, int delta, int defaultValue, int ttlSec) 
			throws TimeoutException, MemcachedException{
		return increment(key, delta, defaultValue, ttlSec, defaultTimeoutMiliSec);
    }
    
	public long decrement(String key, int delta, int defaultValue) 
			throws TimeoutException, MemcachedException{
		return decrement(key, delta, defaultValue, defaultTimeoutMiliSec);
    }
    public long decrement(String key, int delta) 
			throws TimeoutException, MemcachedException{
		return decrement(key, delta, defaultTimeoutMiliSec);
    }	
    public long decrement(String key, int delta, int defaultValue, int ttlSec) 
			throws TimeoutException, MemcachedException{
		return decrement(key, delta, defaultValue, ttlSec, defaultTimeoutMiliSec);
    }

	public boolean touch(String key, int ttlSec) throws TimeoutException {
		return touch(key, ttlSec, defaultTimeoutMiliSec);
	}
	
	public Object gat(String key, int ttlSec) throws TimeoutException {
		return gat(key, ttlSec, defaultTimeoutMiliSec);
	}
}
