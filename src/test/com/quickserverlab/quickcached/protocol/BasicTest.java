package com.quickserverlab.quickcached.protocol;

import java.io.IOException;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import junit.framework.TestCase;
import com.quickserverlab.quickcached.client.MemcachedClient;
import com.quickserverlab.quickcached.client.MemcachedException;
import com.quickserverlab.quickcached.client.TimeoutException;

/**
 *
 * @author akshath
 */
public class BasicTest  extends TestCase  {
	protected MemcachedClient c = null;

	public BasicTest(String name) {
        super(name);
    }
	
	public void setUp(){
		try {
			c = MemcachedClient.getInstance();
			c.setUseBinaryConnection(false);
			
			String serverList = System.getProperty(
				"com.quickserverlab.quickcached.server_list", "localhost:11211");			
			c.setAddresses(serverList);
			
			c.setDefaultTimeoutMiliSec(3000);//3 sec
			c.setConnectionPoolSize(1);
			c.init();
		} catch (Exception ex) {
			Logger.getLogger(TextProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	public void tearDown(){
		if(c!=null) {
			try {
				c.stop();
			} catch (IOException ex) {
				Logger.getLogger(TextProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
	}

    public static void main(String args[]) {
        junit.textui.TestRunner.run(BasicTest.class);
    }

	public void testReplace() throws MemcachedException, TimeoutException {
		String readObject = null;
		String key = null;
		String value = null;
		
		//1
        value = "ABCD";
		key = "testrep1";

		c.set(key, 3600, "World");

		boolean flag = c.replace(key, 3600, value);
		assertTrue(flag);
		readObject = (String) c.get(key);
		assertNotNull(readObject);
		assertEquals("ABCD", readObject);

		c.delete(key);
		
		flag = c.replace(key, 3600, "XYZ");
		assertFalse(flag);
		
		//read old value i.e. no value
		readObject = (String) c.get(key);
		assertNull(readObject);
		
		//2
		key = "testrep2";
		c.set(key, 3600, "World");
		
		Object client = c.getBaseClient();
		if(client instanceof net.rubyeye.xmemcached.MemcachedClient) {
			net.rubyeye.xmemcached.MemcachedClient xmc = (net.rubyeye.xmemcached.MemcachedClient) client;
			try {
				xmc.replaceWithNoReply(key, 3600, value);
			} catch (InterruptedException ex) {
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			} catch (net.rubyeye.xmemcached.exception.MemcachedException ex) {
				Logger.getLogger(BasicTest.class.getName()).log(Level.SEVERE, null, ex);
			} 
			
			readObject = (String) c.get(key);
			assertNotNull(readObject);
			assertEquals("ABCD", readObject);
		} else if (client instanceof net.spy.memcached.MemcachedClient) {			
			//does not support noreply			
		}		
		
		c.delete(key);		
		if(client instanceof net.rubyeye.xmemcached.MemcachedClient) {
			net.rubyeye.xmemcached.MemcachedClient xmc = (net.rubyeye.xmemcached.MemcachedClient) client;
			try {
				xmc.replaceWithNoReply(key, 3600, "XYZ");
			} catch (InterruptedException ex) {
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			} catch (net.rubyeye.xmemcached.exception.MemcachedException ex) {
				Logger.getLogger(BasicTest.class.getName()).log(Level.SEVERE, null, ex);
			} 			
			//read old value i.e. no value
			readObject = (String) c.get(key);
			assertNull(readObject);	
		} else if (client instanceof net.spy.memcached.MemcachedClient) {			
			//does not support noreply			
		}			
	}
}
