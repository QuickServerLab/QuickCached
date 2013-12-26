package com.quickserverlab.quickcached.client.impl;

import net.spy.memcached.internal.OperationFuture;
import com.quickserverlab.quickcached.client.TimeoutException;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import com.quickserverlab.quickcached.client.MemcachedClient;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import net.spy.memcached.CASValue;
import net.spy.memcached.OperationTimeoutException;
import com.quickserverlab.quickcached.client.CASResponse;
import com.quickserverlab.quickcached.client.MemcachedException;

/**
 *
 * @author Akshathkumar Shetty
 */
public class SpyMemcachedImpl extends MemcachedClient {
	private net.spy.memcached.MemcachedClient[] c = null;        
	
	private String hostList;
	private boolean binaryConnection = true;
	
	private int poolSize = 5;
	
	public SpyMemcachedImpl() {
		
	}
        
	public void setUseBinaryConnection(boolean flag) {
		binaryConnection = flag;
	}
	
	public void setConnectionPoolSize(int size) {
		poolSize = size;
	}
	
	public void setAddresses(String list) {
		hostList = list;
	}

	public void addServer(String list) throws IOException{
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public void removeServer(String list) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	public void init() throws IOException {
		if(c!=null) stop();
		
		c = new net.spy.memcached.MemcachedClient[poolSize];
		if(binaryConnection==false) {
			for(int i=0;i<poolSize;i++) {
				c[i] = new net.spy.memcached.MemcachedClient(
					net.spy.memcached.AddrUtil.getAddresses(hostList));
			}
		} else {
			for(int i=0;i<poolSize;i++) {
				c[i] = new net.spy.memcached.MemcachedClient(
					new net.spy.memcached.BinaryConnectionFactory(),
					net.spy.memcached.AddrUtil.getAddresses(hostList));
			}
		}
	}

	public void stop() throws IOException {
		for(int i=0;i<poolSize;i++) {
			c[i].shutdown();
		}
		c = null;
	}
	
	public net.spy.memcached.MemcachedClient getCache() {
		int i = (int) (Math.random()* poolSize);
		return c[i];
	}
	
	public boolean touch(String key, int ttlSec, long timeoutMiliSec) throws TimeoutException {
		Future <Boolean> f = getCache().touch(key, ttlSec);
		Boolean flag = false;
		try {
			flag = (Boolean) f.get(timeoutMiliSec, TimeUnit.MILLISECONDS);
		} catch(Exception e) {
			f.cancel(false);
			throw new TimeoutException("Timeout "+e);
		}
		return flag.booleanValue();
	}
			
	public void set(String key, int ttlSec, Object value, long timeoutMiliSec) 
			throws TimeoutException, MemcachedException {
		Future <Boolean> f = getCache().set(key, ttlSec, value);
		Boolean flag = false;
		try {
			flag = (Boolean) f.get(timeoutMiliSec, TimeUnit.MILLISECONDS);
		} catch(Exception e) {
			f.cancel(false);
			throw new TimeoutException("Timeout "+e);
		} 
	}
	
	public boolean add(String key, int ttlSec, Object value, long timeoutMiliSec) 
			throws TimeoutException {
		Future <Boolean> f = getCache().add(key, ttlSec, value);
		Boolean flag = false;
		try {
			flag = (Boolean) f.get(timeoutMiliSec, TimeUnit.MILLISECONDS);
		} catch(Exception e) {
			f.cancel(false);
			throw new TimeoutException("Timeout "+e);
		}
		return flag.booleanValue();
	}
	
	public boolean replace(String key, int ttlSec, Object value, long timeoutMiliSec) 
			throws TimeoutException {
		Future <Boolean> f = getCache().replace(key, ttlSec, value);
		Boolean flag = false;
		try {
			flag = (Boolean) f.get(timeoutMiliSec, TimeUnit.MILLISECONDS);
		} catch(Exception e) {
			f.cancel(false);
			throw new TimeoutException("Timeout "+e);
		}
		return flag.booleanValue();
	}
	
	public boolean append(String key, Object value, long timeoutMiliSec) 
			throws TimeoutException {
		CASValue casv = getCache().gets(key);		
		Future <Boolean> f = getCache().append(casv.getCas(), key, value);
		Boolean flag = false;
		try {
			flag = (Boolean) f.get(timeoutMiliSec, TimeUnit.MILLISECONDS);
		} catch(Exception e) {
			f.cancel(false);
			throw new TimeoutException("Timeout "+e);
		}
		return flag.booleanValue();
	}
	
	public boolean prepend(String key, Object value, long timeoutMiliSec) 
			throws TimeoutException {
		CASValue casv = getCache().gets(key);
		Future <Boolean> f = getCache().prepend(casv.getCas(),key, value);
		Boolean flag = false;
		try {
			flag = (Boolean) f.get(timeoutMiliSec, TimeUnit.MILLISECONDS);
		} catch(Exception e) {
			f.cancel(false);
			throw new TimeoutException("Timeout "+e);
		}
		return flag.booleanValue();
	}

	public CASResponse cas(String key, Object value, int ttlSec, long cas, long timeoutMiliSec) 
			throws TimeoutException, com.quickserverlab.quickcached.client.MemcachedException{
		try{
			Future <net.spy.memcached.CASResponse> f =  getCache().
				asyncCAS(key, cas, ttlSec, value, getCache().getTranscoder());
			try {
				net.spy.memcached.CASResponse cv = (net.spy.memcached.CASResponse) f.get(timeoutMiliSec, TimeUnit.MILLISECONDS);
				if(cv != null){
					if(cv.equals(net.spy.memcached.CASResponse.OK)){
						return CASResponse.OK;
					}else if(cv.equals(net.spy.memcached.CASResponse.EXISTS)){
						return CASResponse.EXISTS;
					}else if(cv.equals(net.spy.memcached.CASResponse.NOT_FOUND)){
						return CASResponse.NOT_FOUND;
					} else {
						return CASResponse.ERROR;
					}			
				}else{
					return CASResponse.ERROR;
				}
			}catch(CancellationException ce){
				throw new com.quickserverlab.quickcached.client.MemcachedException("CancellationException "+ ce);
			}catch(ExecutionException ee){
				throw new com.quickserverlab.quickcached.client.MemcachedException("ExecutionException "+ ee);
			}catch(InterruptedException ie){
				throw new com.quickserverlab.quickcached.client.MemcachedException("InterruptedException " + ie);
			}catch(java.util.concurrent.TimeoutException te){
				throw new TimeoutException("Timeout "+te);
			}finally{
				f.cancel(false);
			}
		}catch(IllegalStateException ise){
			throw new com.quickserverlab.quickcached.client.MemcachedException("IllegalStateException "+ ise);
		}
	}
	
	public com.quickserverlab.quickcached.client.CASValue gets(String key, long timeoutMiliSec) 
			throws TimeoutException, com.quickserverlab.quickcached.client.MemcachedException{
		com.quickserverlab.quickcached.client.CASValue value = null;
		try{
			OperationFuture <CASValue<Object>> f =  getCache().asyncGets(key);
			try {
				CASValue<Object> cv = (CASValue<Object>) f.get(timeoutMiliSec, TimeUnit.MILLISECONDS);
				if(cv != null){
					value = new com.quickserverlab.quickcached.client.CASValue(cv.getCas(), cv.getValue());
				}else{
					throw new com.quickserverlab.quickcached.client.MemcachedException("Object not found");
				}
			}catch(InterruptedException ie){
				throw new com.quickserverlab.quickcached.client.MemcachedException("InterruptedException "+ ie);
			}catch(ExecutionException ee){
				throw new com.quickserverlab.quickcached.client.MemcachedException("ExecutionException "+ ee);
			}catch(java.util.concurrent.TimeoutException te){
				throw new TimeoutException("Timeout "+ te);
			}finally{
				f.cancel(false);
			}
		}catch(IllegalStateException ise){
			throw new com.quickserverlab.quickcached.client.MemcachedException("IllegalStateException "+ ise);
		}
		return value;				
	}
	
	public Object get(String key, long timeoutMiliSec) throws TimeoutException {
		Object readObject = null;
		Future <Object> f = getCache().asyncGet(key);
		try {
			readObject = f.get(timeoutMiliSec, TimeUnit.MILLISECONDS);
		} catch(Exception e) {
			f.cancel(false);
			throw new TimeoutException("Timeout "+e);
		}
		return readObject;
	}
	
	public Object gat(String key, int ttlSec, long timeoutMiliSec) throws TimeoutException {
		OperationFuture <CASValue<Object>> f = getCache().asyncGetAndTouch(key, ttlSec);
		CASValue<Object> casv = null;
		try {
			casv = f.get(timeoutMiliSec, TimeUnit.MILLISECONDS);
		} catch(Exception e) {
			f.cancel(false);
			throw new TimeoutException("Timeout "+e);
		}		
		return casv.getValue();
	}
        
	/**
	 * Asynchronously get a bunch of objects from the cache
	 */
    public <T> java.util.Map<java.lang.String,T> getBulk(Collection<String> keyCollection, long timeoutMiliSec) 
			throws TimeoutException, com.quickserverlab.quickcached.client.MemcachedException {
		Map<java.lang.String,T> objectList = null;
		try{
			Future <Map<String, Object>> f = getCache().asyncGetBulk(keyCollection);
			try {
				objectList = (Map) f.get(timeoutMiliSec, TimeUnit.MILLISECONDS);
			}catch(CancellationException ce){
				throw new com.quickserverlab.quickcached.client.MemcachedException("CancellationException "+ ce);
			}catch(ExecutionException ee){
				throw new com.quickserverlab.quickcached.client.MemcachedException("ExecutionException "+ ee);
			}catch(InterruptedException ie){
				throw new com.quickserverlab.quickcached.client.MemcachedException("InterruptedException " + ie);
			}catch(java.util.concurrent.TimeoutException te){
				throw new TimeoutException("Timeout "+te);
			}finally{
				f.cancel(false);
			}
		}catch(IllegalStateException ise){
			throw new com.quickserverlab.quickcached.client.MemcachedException("IllegalStateException "+ ise);
		}
		return objectList;
    }                

	public boolean delete(String key, long timeoutMiliSec) throws TimeoutException {
		Future <Boolean> f = getCache().delete(key);
		Boolean flag = false;
		try {
			flag = (Boolean) f.get(timeoutMiliSec, TimeUnit.MILLISECONDS);
		} catch(Exception e) {
			f.cancel(false);
			throw new TimeoutException("Timeout "+e);
		}
		return flag.booleanValue();
	}
    
	public long increment(String key, int delta, long timeoutMiliSec) 
		throws TimeoutException, com.quickserverlab.quickcached.client.MemcachedException {
		long currentValue = 0;
		try{
			OperationFuture <Long> f = getCache().asyncIncr(key, delta);
			try {
				currentValue = (Long) f.get(timeoutMiliSec, TimeUnit.MILLISECONDS);
			}catch(InterruptedException ie){
				throw new com.quickserverlab.quickcached.client.MemcachedException("InterruptedException "+ ie);
			}catch(ExecutionException ee){
				throw new com.quickserverlab.quickcached.client.MemcachedException("ExecutionException "+ ee);
			}catch(java.util.concurrent.TimeoutException te){
				throw new TimeoutException("Timeout "+ te);
			}finally{
				f.cancel(false);
			}
		}catch(IllegalStateException ise){
			throw new com.quickserverlab.quickcached.client.MemcachedException("IllegalStateException "+ ise);
		}
		return currentValue;
	}
	
	/**
	 * Increments the object count
	 *
	 * @param key  
	 * @param value  
	 * @param defaultValue  this value is set if the object doesn't exists
	 * @param timeoutMiliSec  not used
	 * @return new count value of the object
	 */
	public long increment(String key, int delta, long defaultValue, long timeoutMiliSec) 
			throws TimeoutException, com.quickserverlab.quickcached.client.MemcachedException {	
		long newval = -1;
		try{
			newval = getCache().incr(key, delta, defaultValue);
		}catch(OperationTimeoutException ote){
			throw new TimeoutException("Timeout "+ ote);
		}catch(IllegalStateException ise){
			throw new com.quickserverlab.quickcached.client.MemcachedException("IllegalStateException "+ ise);
		}
		return newval;
	}
	
	/**
	 * Increments the object count
	 *
	 * @param key  
	 * @param value  
	 * @param defaultValue  this value is set if the object doesn't exists
	 * @param timeoutMiliSec  not used
	 * @param expiry 
	 * @return new count value of the object
	 */
	public long increment(String key, int delta, long defaultValue, int ttlSec, long timeoutMiliSec) 
			throws TimeoutException, com.quickserverlab.quickcached.client.MemcachedException {
		long newval = -1;
		try{
			newval = getCache().incr(key, delta, defaultValue,ttlSec);
		}catch(OperationTimeoutException ote){
			throw new TimeoutException("Timeout "+ ote);
		}catch(IllegalStateException ise){
			throw new com.quickserverlab.quickcached.client.MemcachedException("IllegalStateException "+ ise);
		}
		return newval;
	}
        
	/**
	 * Increments the object count, without returns
	 * 
	 * @param key  
	 * @param value  
	 * 
	 * 
	 */
	public void incrementWithNoReply(String key, int delta) 
		throws com.quickserverlab.quickcached.client.MemcachedException {    
		long currentValue = 0;
		try{
			OperationFuture <Long> f = getCache().asyncIncr(key, delta);
			try {
				currentValue = (Long) f.get();
			}catch(InterruptedException ie){
				throw new com.quickserverlab.quickcached.client.MemcachedException("InterruptedException "+ ie);
			}catch(ExecutionException ee){
				throw new com.quickserverlab.quickcached.client.MemcachedException("ExecutionException "+ ee);
			}finally{
				f.cancel(false);
			}
		}catch(IllegalStateException ise){
			throw new com.quickserverlab.quickcached.client.MemcachedException("IllegalStateException "+ ise);
		}
	}
    
    
	public long decrement(String key, int delta, long timeoutMiliSec) 
			throws TimeoutException, com.quickserverlab.quickcached.client.MemcachedException {
		long currentValue = 0;
		try{
			OperationFuture <Long> f = getCache().asyncDecr(key, delta);
			try {
				currentValue = (Long) f.get(timeoutMiliSec, TimeUnit.MILLISECONDS);
			}catch(InterruptedException ie){
				throw new com.quickserverlab.quickcached.client.MemcachedException("InterruptedException "+ ie);
			}catch(ExecutionException ee){
				throw new com.quickserverlab.quickcached.client.MemcachedException("ExecutionException "+ ee);
			}catch(java.util.concurrent.TimeoutException te){
				throw new TimeoutException("Timeout "+ te);
			}finally{
				f.cancel(false);
			}
		}catch(IllegalStateException ise){
			throw new com.quickserverlab.quickcached.client.MemcachedException("IllegalStateException "+ ise);
		}
		return currentValue;		
	}
	
	public long decrement(String key, int delta, long defaultValue, long timeoutMiliSec) 
			throws TimeoutException, com.quickserverlab.quickcached.client.MemcachedException {	
		long newval = -1;
		try{
			newval = getCache().decr(key, delta, defaultValue);
		}catch(OperationTimeoutException ote){
			throw new TimeoutException("Timeout "+ ote);
		}catch(IllegalStateException ise){
			throw new com.quickserverlab.quickcached.client.MemcachedException("IllegalStateException "+ ise);
		}
		return newval;
	}
	
	public long decrement(String key, int delta, long defaultValue, int ttlSec, long timeoutMiliSec) 
			throws TimeoutException, com.quickserverlab.quickcached.client.MemcachedException {
		long newval = -1;
		try{
			newval = getCache().decr(key, delta, defaultValue,ttlSec);
		}catch(OperationTimeoutException ote){
			throw new TimeoutException("Timeout "+ ote);
		}catch(IllegalStateException ise){
			throw new com.quickserverlab.quickcached.client.MemcachedException("IllegalStateException "+ ise);
		}
		return newval;
	}
        
	public void decrementWithNoReply(String key, int delta) 
		throws com.quickserverlab.quickcached.client.MemcachedException {    
		long currentValue = 0;
		try{
			OperationFuture <Long> f = getCache().asyncIncr(key, delta);
			try {
				currentValue = (Long) f.get();
			}catch(InterruptedException ie){
				throw new com.quickserverlab.quickcached.client.MemcachedException("InterruptedException "+ ie);
			}catch(ExecutionException ee){
				throw new com.quickserverlab.quickcached.client.MemcachedException("ExecutionException "+ ee);
			}finally{
				f.cancel(false);
			}
		}catch(IllegalStateException ise){
			throw new com.quickserverlab.quickcached.client.MemcachedException("IllegalStateException "+ ise);
		}
	}
    	
	public void flushAll() throws TimeoutException {
		for(int i=0;i<poolSize;i++) {
			c[i].flush();
		}
	}
	
	public Object getBaseClient() {
		return getCache();
	}
        
	public Map getStats() throws Exception {
		return getCache().getStats();
	}
	
	public Map getVersions() throws TimeoutException {
		return getCache().getVersions();
	}
	
}
