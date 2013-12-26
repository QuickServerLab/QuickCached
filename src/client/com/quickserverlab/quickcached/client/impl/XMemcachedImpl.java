package com.quickserverlab.quickcached.client.impl;

import java.io.IOException;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.command.BinaryCommandFactory;
import net.rubyeye.xmemcached.exception.MemcachedException;
import net.rubyeye.xmemcached.utils.AddrUtil;
import com.quickserverlab.quickcached.client.MemcachedClient;
import com.quickserverlab.quickcached.client.TimeoutException;
import java.util.Map;
import net.rubyeye.xmemcached.GetsResponse;
import com.quickserverlab.quickcached.client.CASResponse;
import com.quickserverlab.quickcached.client.CASValue;

/**
 *
 * @author Akshathkumar Shetty
 */
public class XMemcachedImpl extends MemcachedClient {
	private net.rubyeye.xmemcached.MemcachedClient c = null;
	private String hostList;
	private boolean binaryConnection = true;
	private int poolSize = 10;

	public void setUseBinaryConnection(boolean flag) {
		binaryConnection = flag;
	}
	
	public void setConnectionPoolSize(int size) {
		poolSize = size;
	}

	public void setAddresses(String list) {
		hostList = list;
	}

	public void addServer(String list) throws IOException {
		c.addServer(list);
	}

	public void removeServer(String list) {
		c.removeServer(list);
	}

	public void init() throws IOException {
		if(c!=null) stop();
		
		MemcachedClientBuilder builder = new XMemcachedClientBuilder(
				AddrUtil.getAddresses(hostList));
		if(binaryConnection) {
			builder.setCommandFactory(new BinaryCommandFactory());
		}
		builder.setConnectionPoolSize(poolSize);
		c = builder.build();
	}

	public void stop() throws IOException {
		c.shutdown();
		c = null;
	}

	public void set(String key, int ttlSec, Object value, long timeoutMiliSec) 
			throws TimeoutException, 
			com.quickserverlab.quickcached.client.MemcachedException {
		try {
			c.set(key, ttlSec, value, timeoutMiliSec);
		} catch (java.util.concurrent.TimeoutException ex) {
			throw new TimeoutException("Timeout "+ex);
		} catch (InterruptedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"InterruptedException:", ex);
		} catch (MemcachedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"MemcachedException", ex);
			throw new com.quickserverlab.quickcached.client.MemcachedException(
				"MemcachedException: "+ex.getMessage());
		}		
	}
	
	public boolean touch(String key, int ttlSec, long timeoutMiliSec) throws TimeoutException {
		boolean flag = false;
		try {
			flag = c.touch(key, ttlSec, timeoutMiliSec);
		} catch (java.util.concurrent.TimeoutException ex) {
			throw new TimeoutException("Timeout "+ex);
		} catch (InterruptedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"InterruptedException:", ex);
		} catch (MemcachedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"MemcachedException", ex);
		}
		return flag;
	}
	
	public boolean add(String key, int ttlSec, Object value, long timeoutMiliSec) 
			throws TimeoutException {
		boolean flag = false;
		try {
			flag = c.add(key, ttlSec, value, timeoutMiliSec);
		} catch (java.util.concurrent.TimeoutException ex) {
			throw new TimeoutException("Timeout "+ex);
		} catch (InterruptedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"InterruptedException:", ex);
		} catch (MemcachedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"MemcachedException", ex);
		}
		return flag;
	}
	
	public boolean replace(String key, int ttlSec, Object value, long timeoutMiliSec) 
			throws TimeoutException {
		boolean flag = false;
		try {
			flag = c.replace(key, ttlSec, value, timeoutMiliSec);
		} catch (java.util.concurrent.TimeoutException ex) {
			throw new TimeoutException("Timeout "+ex);
		} catch (InterruptedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"InterruptedException:", ex);
		} catch (MemcachedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"MemcachedException", ex);
		}
		return flag;
	}
	
	public boolean append(String key, Object value, long timeoutMiliSec) 
			throws TimeoutException {
		boolean flag = false;
		try {
			flag = c.append(key, value, timeoutMiliSec);
		} catch (java.util.concurrent.TimeoutException ex) {
			throw new TimeoutException("Timeout "+ex);
		} catch (InterruptedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"InterruptedException:", ex);
		} catch (MemcachedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"MemcachedException", ex);
		}	
		return flag;
	}
	
	public boolean prepend(String key, Object value, long timeoutMiliSec) 
			throws TimeoutException {
		boolean flag = false;
		try {
			flag = c.prepend(key, value, timeoutMiliSec);
		} catch (java.util.concurrent.TimeoutException ex) {
			throw new TimeoutException("Timeout "+ex);
		} catch (InterruptedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"InterruptedException:", ex);
		} catch (MemcachedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"MemcachedException", ex);
		}	
		return flag;
	}
	
	public CASResponse cas(String key, Object value, int ttlSec, long cas, long timeoutMiliSec) 
			throws TimeoutException, com.quickserverlab.quickcached.client.MemcachedException{
		try {
			if(c.cas(key, ttlSec, value, timeoutMiliSec, cas)) return CASResponse.OK;
		} catch (java.util.concurrent.TimeoutException ex) {
			throw new TimeoutException("Timeout "+ex);
		} catch (InterruptedException ie) {
			throw new com.quickserverlab.quickcached.client.MemcachedException("InterruptedException " + ie);
		} catch (MemcachedException me) {
			throw new com.quickserverlab.quickcached.client.MemcachedException("MemcachedException " + me);
		}
		return CASResponse.ERROR;
	}
	
	public com.quickserverlab.quickcached.client.CASValue gets(String key, long timeoutMiliSec) 
			throws TimeoutException, com.quickserverlab.quickcached.client.MemcachedException{
		com.quickserverlab.quickcached.client.CASValue value = null;
		try {
			GetsResponse<Object> result = c.gets(key, timeoutMiliSec);
			if(result != null){
				value = new CASValue(result.getCas(), result.getValue());
			}
		} catch (java.util.concurrent.TimeoutException ex) {
			throw new TimeoutException("Timeout "+ex);
		} catch (InterruptedException ex) {
			throw new com.quickserverlab.quickcached.client.MemcachedException("InterruptedException "+ex);
		} catch (MemcachedException ex) {
			throw new com.quickserverlab.quickcached.client.MemcachedException("MemcachedException "+ex);
		}
		return value;
	}

	public Object gat(String key, int ttlSec, long timeoutMiliSec) throws TimeoutException {
		Object readObject = null;
		try {
			readObject = c.getAndTouch(key, ttlSec, timeoutMiliSec);
		} catch(java.util.concurrent.TimeoutException ex) {
			throw new TimeoutException("Timeout "+ex);
		} catch(InterruptedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"InterruptedException:", ex);
		} catch(MemcachedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"MemcachedException", ex);
		}
		return readObject;
	}

	public Object get(String key, long timeoutMiliSec) throws TimeoutException {
		Object readObject = null;
		try {
			readObject = c.get(key, timeoutMiliSec);			
		} catch(java.util.concurrent.TimeoutException ex) {
			throw new TimeoutException("Timeout "+ex);
		} catch(InterruptedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"InterruptedException:", ex);
		} catch(MemcachedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"MemcachedException", ex);
		}
		
		return readObject;
	}

    public <T> java.util.Map<java.lang.String,T> getBulk(Collection<String> keyCollection, long timeoutMiliSec) 
			throws TimeoutException, com.quickserverlab.quickcached.client.MemcachedException{
		Map<java.lang.String,T> objectList = null;
		try {
			objectList = c.get(keyCollection, timeoutMiliSec);			
		} catch(java.util.concurrent.TimeoutException ex) {
			throw new TimeoutException("Timeout "+ex);
		} catch(InterruptedException ex) {
			throw new TimeoutException("InterruptedException "+ex);
		} catch(MemcachedException ex) {
			throw new TimeoutException("MemcachedException "+ex);
		}
		return objectList;
	}
                
	public boolean delete(String key, long timeoutMiliSec) throws TimeoutException {
		try {
			return c.delete(key, timeoutMiliSec);
		} catch(java.util.concurrent.TimeoutException ex) {
			throw new TimeoutException("Timeout "+ex);
		} catch(InterruptedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"InterruptedException:", ex);
		} catch(MemcachedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"MemcachedException", ex);
		}
		return false;
	}

	public void flushAll() throws TimeoutException {
		try {
			c.flushAll();
		} catch (java.util.concurrent.TimeoutException ex) {
			throw new TimeoutException("Timeout "+ex);
		} catch (InterruptedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, null, ex);
		} catch (MemcachedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	public Object getBaseClient() {
		return c;
	}
        
	public Map getStats() throws Exception {
		return c.getStats();
	}
	
	/**
	 * Increments the object count, this method doesn't accept default value
	 * "increment" are used to change data for some item in-place, incrementing it. The data for the item is 
	 * treated as decimal representation of a 64-bit unsigned integer. If the current data value does not
	 * conform to such a representation, the commands behave as if the value were 0. Also, the item must
	 * already exist for "increment" to work; these commands won't pretend that a non-existent key exists with 
	 * value 0; 
	 * 
	 * 
	 * @param key  
	 * @param value Increment by this value(delta) 
	 * @param timeoutMiliSec  
	 * @return new count value of the object
	 */ 
	public long increment(String key, int delta, long timeoutMiliSec) 
			throws TimeoutException, com.quickserverlab.quickcached.client.MemcachedException{
		long newval = -1;        
		try {
			newval = c.incr(key, delta);
			if(newval==-1) {
				throw new TimeoutException("Timeout ");
			}
			return newval;
		} catch (java.util.concurrent.TimeoutException ex) {
			throw new TimeoutException("Timeout "+ex);
		} catch (InterruptedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"InterruptedException:", ex);
			throw new TimeoutException("InterruptedException:"+ ex);
		} catch (MemcachedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"MemcachedException", ex);
			throw new TimeoutException("MemcachedException:"+ ex);
		}	
	}
	
	/**
	 * Increments the object count
	 *
	 * @param key  
	 * @param value  Increment by this value(delta) 
	 * @param defaultValue  this value is set if the object doesn't exists
	 * @param timeoutMiliSec  
	 * @return new count value of the object
	 */
	public long increment(String key, int delta, long defaultValue, long timeoutMiliSec) 
		throws TimeoutException, com.quickserverlab.quickcached.client.MemcachedException {
		long newval = -1;        
		try {
			newval = c.incr(key, delta);
			if(newval==-1) {
				throw new TimeoutException("Timeout ");
			}
		} catch (java.util.concurrent.TimeoutException ex) {
			throw new TimeoutException("Timeout "+ex);
		} catch (InterruptedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"InterruptedException:", ex);
			throw new TimeoutException("InterruptedException:"+ ex);
		} catch (MemcachedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"MemcachedException", ex);
			throw new TimeoutException("MemcachedException:"+ ex);
		}
		return newval;
	}
	
	/**
	 * Increments the object count
	 *
	 * @param key  
	 * @param value  
	 * @param defaultValue  this value is set if the object doesn't exists 
	 * @param ttlSec Time to leave in sec 
	 * @param timeoutMiliSec 
	 * @return new count value of the object
	 */
	public long increment(String key, int delta, long defaultValue, int ttlSec, long timeoutMiliSec) 
			throws TimeoutException, com.quickserverlab.quickcached.client.MemcachedException {
		long newval = -1;
		try {
			newval = c.incr(key, delta);
			if(newval==-1) {
				throw new TimeoutException("Timeout ");
			}
		} catch (java.util.concurrent.TimeoutException ex) {
			throw new TimeoutException("Timeout "+ex);
		} catch (InterruptedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"InterruptedException:", ex);
		} catch (MemcachedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"MemcachedException", ex);
		}
		return newval;
	}

	/**
	 * Increments the object count, doesn't return the current count value.
	 * The item must already exist for "increment" to work; these commands won't pretend 
	 * that a non-existent key exists with value 0; 
	 * 
	 * Object is not created if object doesn't exists. 
	 * 
	 * @param key  
	 * @param value  - Delta value to increment the object count.
	 * 
	 */
	public void incrementWithNoReply(String key, int delta) 
			throws com.quickserverlab.quickcached.client.MemcachedException{      
		try {
			c.incrWithNoReply(key, delta);
		} catch (InterruptedException ex) {
			throw new com.quickserverlab.quickcached.client.MemcachedException("InterruptedException "+ex);
		} catch (MemcachedException ex) {
			throw new com.quickserverlab.quickcached.client.MemcachedException("MemcachedException "+ex);
	}
	}

	public long decrement(String key, int delta, long timeoutMiliSec) 
			throws TimeoutException, com.quickserverlab.quickcached.client.MemcachedException{
		long newval = -1;   
		try {
			newval = c.decr(key, delta, timeoutMiliSec);
			if(newval==-1) {
				throw new TimeoutException("Timeout ");
	}
		} catch (java.util.concurrent.TimeoutException ex) {
			throw new TimeoutException("Timeout "+ex);
		} catch (InterruptedException ex) {
			throw new com.quickserverlab.quickcached.client.MemcachedException("InterruptedException "+ex);
		} catch (MemcachedException ex) {
			throw new com.quickserverlab.quickcached.client.MemcachedException("MemcachedException "+ex);
		}
		return newval;
	}

	public long decrement(String key, int delta, long defaultValue, long timeoutMiliSec) 
		throws TimeoutException, com.quickserverlab.quickcached.client.MemcachedException {
		long newval = -1;   
		try {
			newval = c.decr(key, delta, defaultValue, timeoutMiliSec);
			if(newval==-1) {
				throw new TimeoutException("Timeout ");
			}
		} catch (java.util.concurrent.TimeoutException ex) {
			throw new TimeoutException("Timeout "+ex);
		} catch (InterruptedException ex) {
			throw new com.quickserverlab.quickcached.client.MemcachedException("InterruptedException "+ex);
		} catch (MemcachedException ex) {
			throw new com.quickserverlab.quickcached.client.MemcachedException("MemcachedException "+ex);
		}
		return newval;		
	}

	public long decrement(String key, int delta, long defaultValue, int ttlSec, long timeoutMiliSec) 
			throws TimeoutException, com.quickserverlab.quickcached.client.MemcachedException {
		long newval = -1;   
		try {
			newval = c.decr(key, delta, defaultValue, timeoutMiliSec, ttlSec);
			if(newval==-1) {
				throw new TimeoutException("Timeout ");
			}
		} catch (java.util.concurrent.TimeoutException ex) {
			throw new TimeoutException("Timeout "+ex);
		} catch (InterruptedException ex) {
			throw new com.quickserverlab.quickcached.client.MemcachedException("InterruptedException "+ex);
		} catch (MemcachedException ex) {
			throw new com.quickserverlab.quickcached.client.MemcachedException("MemcachedException "+ex);
		}
		return newval;
	}

	public void decrementWithNoReply(String key, int delta) 
			throws com.quickserverlab.quickcached.client.MemcachedException{      
		try {
			c.decrWithNoReply(key, delta);
		} catch (InterruptedException ex) {
			throw new com.quickserverlab.quickcached.client.MemcachedException("InterruptedException "+ex);
		} catch (MemcachedException ex) {
			throw new com.quickserverlab.quickcached.client.MemcachedException("MemcachedException "+ex);
		}		
	}

	public Map getVersions() throws TimeoutException {
		try {
			return c.getVersions();
		} catch (java.util.concurrent.TimeoutException ex) {
			throw new TimeoutException("Timeout "+ex);
		} catch (InterruptedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"InterruptedException:", ex);
		} catch (MemcachedException ex) {
			Logger.getLogger(XMemcachedImpl.class.getName()).log(Level.SEVERE, 
					"MemcachedException", ex);
		}
		return null;
	}
}
