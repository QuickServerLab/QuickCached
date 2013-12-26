package com.quickserverlab.quickcached.protocol;

import java.io.IOException;
import java.util.logging.*;
import com.quickserverlab.quickcached.client.MemcachedClient;
import java.util.Date;
import com.quickserverlab.quickcached.client.TimeoutException;
import com.quickserverlab.quickcached.client.MemcachedException;
/**
 *
 * @author akshath
 */
public class BinaryProtocolTest extends ProtocolTest {
	public BinaryProtocolTest(String name) {
        super(name);
    }

	public void setUp(){
		try {
			c = MemcachedClient.getInstance();
			c.setUseBinaryConnection(true);
			
			String serverList = System.getProperty(
				"com.quickserverlab.quickcached.server_list", "localhost:11211");			
			c.setAddresses(serverList);
			
			c.setDefaultTimeoutMiliSec(3000);//3 sec
			c.setConnectionPoolSize(1);
			c.init();
		} catch (Exception ex) {
			Logger.getLogger(BinaryProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	public void tearDown(){
		if(c!=null) {
			try {
				c.stop();
			} catch (IOException ex) {
				Logger.getLogger(BinaryProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
	}

    public static void main(String args[]) {
        junit.textui.TestRunner.run(BinaryProtocolTest.class);
    }
	
	public void testTouch() throws MemcachedException, TimeoutException {
		String readObject = null;
		String key = null;
		String value = null;
		
		//1
		key = "testtuc1";
        Date datevalue = new Date();

		c.set(key, 50, datevalue);
		Date readObjectDate = (Date) c.get(key);

		assertNotNull(readObjectDate);
		assertEquals(datevalue.getTime(),  readObjectDate.getTime());
		
		c.touch(key, 3600);
		
		readObjectDate = (Date) c.get(key);

		assertNotNull(readObjectDate);
		assertEquals(datevalue.getTime(),  readObjectDate.getTime());
	
		//2
		key = "testtuc2";
		c.set(key, 50, "World");
		readObject = (String) c.get(key);

		assertNotNull(readObject);
		assertEquals("World",  readObject);
		
		c.touch(key, 3600);
		
		readObject = (String) c.get(key);

		assertNotNull(readObject);
		assertEquals("World",  readObject);
	}
	
	public void testGat() throws TimeoutException, MemcachedException {		
		String readObject = null;
		String key = null;
		String value = null;
		
		//1
		key = "testgat1";
		c.set(key, 50, "World");
		readObject = (String) c.gat(key, 3600);

		assertNotNull(readObject);
		assertEquals("World",  readObject);	
	
		//2
		key = "testgat2";
		Date datevalue = new Date();
		c.set(key, 50, datevalue);
		Date readObjectDate = (Date) c.gat(key, 3600);

		assertNotNull(readObjectDate);
		assertEquals(datevalue.getTime(),  readObjectDate.getTime());
	}
}
