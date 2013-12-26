package com.quickserverlab.quickcached.protocol;

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import junit.framework.TestCase;

import com.quickserverlab.quickcached.client.*;
import com.quickserverlab.quickcached.client.impl.QuickCachedClientImpl;
import com.quickserverlab.quickcached.client.impl.SpyMemcachedImpl;
import com.quickserverlab.quickcached.client.impl.XMemcachedImpl;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import net.rubyeye.xmemcached.XMemcachedClient;

/**
 *
 * @author akshath
 */
public class ProtocolTest extends TestCase  {
	protected MemcachedClient c = null;

	public ProtocolTest(String name) {
        super(name);
    }

	public void testGet() throws TimeoutException, MemcachedException {
		String readObject = null;
		String key = null;
		//1 - String
		key = "testget1";
		c.set(key, 3600, "World");
		readObject = (String) c.get(key);
		
		assertNotNull(readObject);
		assertEquals("World",  readObject);
    
		//2 - native obj
        Date value = new Date();
		key = "testget2";
		c.set(key, 3600, value);
		Date readObjectDate = (Date) c.get(key);

		assertNotNull(readObjectDate);
		assertEquals(value.getTime(),  readObjectDate.getTime());
		
		//3 - custom obj
        TestObject testObject = new TestObject();
		testObject.setName(key);
		key = "testget3";
		c.set(key, 3600, testObject);
		TestObject readTestObject = (TestObject) c.get(key);

		assertNotNull(readTestObject);
		assertEquals(testObject.getName(),  readTestObject.getName());
		
		//4 - no reply
		Object client = c.getBaseClient();
		key = "testget4";
		if(client instanceof net.rubyeye.xmemcached.MemcachedClient) {
			net.rubyeye.xmemcached.MemcachedClient xmc = (net.rubyeye.xmemcached.MemcachedClient) client;
			
			try {
				xmc.setWithNoReply(key, 3600, "World");			
			} catch (InterruptedException ex) {
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			} catch (net.rubyeye.xmemcached.exception.MemcachedException ex) {
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			} 			
			readObject = (String) c.get(key);
			assertNotNull(readObject);
			assertEquals("World",  readObject);
		} else if (client instanceof net.spy.memcached.MemcachedClient) {
			net.spy.memcached.MemcachedClient smc = (net.spy.memcached.MemcachedClient) client;
			
			//does not support noreply
		}
		
	}

	public void testGetBulk() throws TimeoutException, MemcachedException {
		String readObject = null;
		int numberOfKeys = 10;
		String key = null;
		String value = null;
		
		Map<String, Object> testRequest = new HashMap<String, Object>();
		ArrayList<String> testKeys = new ArrayList<String>();
		
		for (int i = 0; i < numberOfKeys; i++) {
			key = "key" + System.nanoTime();
			value = "val" + System.nanoTime();

			c.set(key, 3600, value);
			testRequest.put(key, value);
			testKeys.add(key);
			
			//Confirm insertion
			readObject = (String) c.get(key);
			assertNotNull(readObject);
			assertEquals(value,  readObject);
		}
		
		try {
			int resultCount = 0;
            Map<String, Object> testResult = c.getBulk(testKeys);
			for(String keys : testKeys){
				resultCount++;
				assertEquals(testRequest.get(keys),  testResult.get(keys));
			}
			assertEquals(resultCount,  numberOfKeys);
        } catch (Exception e) {
            e.printStackTrace();
			assertFalse("Exception occured in get bulk", true);
        }
		
	}

	public void testGets() throws TimeoutException, MemcachedException {
		String readObject = null;
		String key = "testGetCAS";
		String value = "Value";
		
		long casVal = 1; //default cas value is 1, and increments on each set
		
		c.set(key, 3600, value);
		CASValue casResult = c.gets(key, 1000);
		
		assertNotNull(casResult);
		assertEquals(casResult.getValue(), value);
		assertEquals(casResult.getCas(), casVal);
		
		casVal++;
		value = "Value2";
		
		c.set(key, 3600, value);
		CASValue casResult2 = c.gets(key, 1000);
		
		assertNotNull(casResult2);
		assertEquals(casResult2.getValue(), value);
		assertEquals(casResult2.getCas(), casVal);
	}
	
	public void testCAS() throws TimeoutException, MemcachedException {
		String key = "testcas1";
		
		
		Object client = c.getBaseClient();
		if(client instanceof net.rubyeye.xmemcached.MemcachedClient) {
			//ERROR
			CASResponse testRslt1 = c.cas(key, "value", 3000, 1);
			assertNotNull(testRslt1);
			assertEquals(CASResponse.ERROR,  testRslt1);

			String result = (String)c.get(key);
			assertNull(result);
		} else if (client instanceof com.quickserverlab.quickcached.client.impl.QuickCachedClientImpl) {			
			//NOT_FOUND
			CASResponse testRslt1 = c.cas(key, "value", 3000, 1);
			assertNotNull(testRslt1);
			assertEquals(CASResponse.NOT_FOUND,  testRslt1);

			String result = (String)c.get(key);
			assertNull(result);
		}
		
		c.set(key, 3000, "value");
		//Default value of cas is 1
		CASResponse testRslt2 = c.cas(key, "value2", 3000, 1);
		assertNotNull(testRslt2);
		assertEquals(CASResponse.OK,  testRslt2);
		
		//Increment the cas, it should match the current cas, so saving current CAS
		CASResponse testRslt3 = c.cas(key, "value2", 3000, 2);
		assertNotNull(testRslt3);
		assertEquals(CASResponse.OK,  testRslt3);
		
		if(client instanceof net.rubyeye.xmemcached.MemcachedClient) {
			//Increment the cas, passing wrong CAS value, expecting rslt as "ERROR"
			CASResponse testRslt4 = c.cas(key, "value3", 3000, 6);
			assertNotNull(testRslt4);
			assertEquals(CASResponse.ERROR,  testRslt4);
		} else if (client instanceof com.quickserverlab.quickcached.client.impl.QuickCachedClientImpl) {			
			//Increment the cas, passing wrong CAS value, expecting rslt as "EXISTS"
			CASResponse testRslt4 = c.cas(key, "value3", 3000, 6);
			assertNotNull(testRslt4);
			assertEquals(CASResponse.EXISTS,  testRslt4);
		}
	}
	
	public void testAppend() throws TimeoutException, MemcachedException {
		String readObject = null;
		String key = null;
		String value = null;
		
		//1
		key = "testapp1";
		c.set(key, 3600, "ABCD");

		c.append(key, "EFGH");
		readObject = (String) c.get(key);

		assertNotNull(readObject);
		assertEquals("ABCDEFGH", readObject);
		
		//2
		Object client = c.getBaseClient();
		if(client instanceof net.rubyeye.xmemcached.MemcachedClient) {
			net.rubyeye.xmemcached.MemcachedClient xmc = (net.rubyeye.xmemcached.MemcachedClient) client;
			key = "testapp1";
			try {
				xmc.appendWithNoReply(key,"XYZ");				
			} catch (InterruptedException ex) {
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			} catch (net.rubyeye.xmemcached.exception.MemcachedException ex) {
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			} 			
			readObject = (String) c.get(key);
			assertNotNull(readObject);
			assertEquals("ABCDEFGHXYZ", readObject);
		} else if (client instanceof net.spy.memcached.MemcachedClient) {			
			//does not support noreply			
		}		
	}

	public void testPrepend() throws TimeoutException, MemcachedException {
		String readObject = null;
		String key = null;
		String value = null;
		
		key = "testpre1";

		//1
		c.set(key, 3600, "ABCD");
		c.prepend(key, "EFGH");
		readObject = (String) c.get(key);

		assertNotNull(readObject);
		assertEquals("EFGHABCD", readObject);
		
		//2
		Object client = c.getBaseClient();
		if(client instanceof net.rubyeye.xmemcached.MemcachedClient) {
			net.rubyeye.xmemcached.MemcachedClient xmc = (net.rubyeye.xmemcached.MemcachedClient) client;
			key = "testpre1";
			try {
				xmc.prependWithNoReply(key,"XYZ");				
			} catch (InterruptedException ex) {
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			} catch (net.rubyeye.xmemcached.exception.MemcachedException ex) {
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			} 			
			readObject = (String) c.get(key);
			assertNotNull(readObject);
			assertEquals("XYZEFGHABCD", readObject);
		} else if (client instanceof net.spy.memcached.MemcachedClient) {			
			//does not support noreply			
		}
	}

	public void testAdd() throws TimeoutException, MemcachedException {
		String readObject = null;
		String key = null;
		String value = null;
		
		//1
        value = "ABCD";
		key = "testadd1";

		c.delete(key);
		boolean flag = c.add(key, 3600, value);
		assertTrue(flag);
		
		readObject = (String) c.get(key);
		assertNotNull(readObject);
		assertEquals("ABCD", readObject);

		
		flag = c.add(key, 3600, "XYZ");
		assertFalse(flag);
		
		//read old value
		readObject = (String) c.get(key);
		assertNotNull(readObject);
		assertEquals("ABCD", readObject);
		
		//2
		key = "testadd2";
		c.delete(key);
		Object client = c.getBaseClient();
		if(client instanceof net.rubyeye.xmemcached.MemcachedClient) {
			net.rubyeye.xmemcached.MemcachedClient xmc = (net.rubyeye.xmemcached.MemcachedClient) client;
			try {
				xmc.addWithNoReply(key, 3600, value);
			} catch (InterruptedException ex) {
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			} catch (net.rubyeye.xmemcached.exception.MemcachedException ex) {
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			} 			
			readObject = (String) c.get(key);
			assertNotNull(readObject);
			assertEquals("ABCD", readObject);

			flag = c.add(key, 3600, "XYZ");
			assertFalse(flag);

			//read old value
			readObject = (String) c.get(key);
			assertNotNull(readObject);
			assertEquals("ABCD", readObject);
		} else if (client instanceof net.spy.memcached.MemcachedClient) {			
			//does not support noreply			
		}
		
	}

	public void testReplace() throws TimeoutException, MemcachedException {
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
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
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
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			} 
			
			//read old value i.e. no value
			readObject = (String) c.get(key);
			assertNull(readObject);	
		} else if (client instanceof net.spy.memcached.MemcachedClient) {			
			//does not support noreply			
		}			
	}

	public void testIncrement() throws TimeoutException, MemcachedException {
		String readObject = null;
		String key = null;
		String value = null;
		
		//1
		key = "testinc1";
        value = "10";		

		c.set(key, 3600, value);
		c.increment(key, 10);

		readObject = (String) c.get(key);
		assertNotNull(readObject);
		assertEquals("20", readObject);

		c.increment(key, 1);
		readObject = (String) c.get(key);
		assertNotNull(readObject);
		assertEquals("21", readObject);
		
		//2
		Object client = c.getBaseClient();
		if(client instanceof net.rubyeye.xmemcached.MemcachedClient) {
			net.rubyeye.xmemcached.MemcachedClient xmc = (net.rubyeye.xmemcached.MemcachedClient) client;
			try {
				xmc.incrWithNoReply(key, 4);
			} catch (InterruptedException ex) {
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			} catch (net.rubyeye.xmemcached.exception.MemcachedException ex) {
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			} 
			
			readObject = (String) c.get(key);
			assertNotNull(readObject);
			assertEquals("25", readObject);
		} else if (client instanceof net.spy.memcached.MemcachedClient) {			
			//does not support noreply
		}		
	}

	public void testDecrement() throws TimeoutException, MemcachedException {
		String readObject = null;
		String key = null;
		String value = null;
		
        //1
		key = "testdec1";
        value = "25";		

		c.set(key, 3600, value);
		c.decrement(key, 10);

		readObject = (String) c.get(key);
		assertNotNull(readObject);
		assertEquals("15", readObject);

		c.decrement(key, 1);
		readObject = (String) c.get(key);
		assertNotNull(readObject);
		assertEquals("14", readObject);
		
		//2
		Object client = c.getBaseClient();
		if(client instanceof net.rubyeye.xmemcached.MemcachedClient) {
			net.rubyeye.xmemcached.MemcachedClient xmc = (net.rubyeye.xmemcached.MemcachedClient) client;
			try {
				xmc.decrWithNoReply(key, 4);
			} catch (InterruptedException ex) {
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			} catch (net.rubyeye.xmemcached.exception.MemcachedException ex) {
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			} 			
			readObject = (String) c.get(key);
			assertNotNull(readObject);
			assertEquals("10", readObject);
		} else if (client instanceof net.spy.memcached.MemcachedClient) {			
			//does not support noreply
		}
		
	}

	public void testDelete() throws TimeoutException, MemcachedException {
		String readObject = null;
		String key = null;
		String value = null;
		
		//1
		key = "testdel1";
		
		c.set(key, 3600, "World");
		readObject = (String) c.get(key);

		assertNotNull(readObject);
		assertEquals("World",  readObject);

		c.delete(key);
		readObject = (String) c.get(key);
		assertNull(readObject);
		
		//2
		c.set(key, 3600, "World");
		readObject = (String) c.get(key);

		assertNotNull(readObject);
		assertEquals("World",  readObject);
		Object client = c.getBaseClient();
		if(client instanceof net.rubyeye.xmemcached.MemcachedClient) {
			net.rubyeye.xmemcached.MemcachedClient xmc = (net.rubyeye.xmemcached.MemcachedClient) client;
			try {
				xmc.deleteWithNoReply(key);
			} catch (InterruptedException ex) {
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			} catch (net.rubyeye.xmemcached.exception.MemcachedException ex) {
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			} 
			
			readObject = (String) c.get(key);
			assertNull(readObject);
		} else if (client instanceof net.spy.memcached.MemcachedClient) {			
			//does not support noreply
		}
		
	}

	public void testVersion() throws TimeoutException {
		Map ver = c.getVersions();
		assertNotNull(ver);
		//System.out.println("ver: "+ver);
		Iterator iterator = ver.keySet().iterator();
		InetSocketAddress key = null;
		while(iterator.hasNext()) {
			key = (InetSocketAddress) iterator.next();
			assertNotNull(key);
			//assertEquals("1.4.5",  (String) ver.get(key));
		}
	}

	public void testStats() throws Exception {
		Map stats = c.getStats();
		assertNotNull(stats);

		Iterator iterator = stats.keySet().iterator();
		InetSocketAddress key = null;
		while(iterator.hasNext()) {
			key = (InetSocketAddress) iterator.next();
			assertNotNull(key);
			//System.out.println("Stat for "+key+" " +stats.get(key));
		}
	}

	public void testFlush() throws TimeoutException, MemcachedException {
		String readObject = null;
		String key = null;
		String value = null;
		
		//1
		key = "testflush1";
		c.set(key, 3600, "World");
		readObject = (String) c.get(key);

		assertNotNull(readObject);
		assertEquals("World",  readObject);

		c.flushAll();

		readObject = (String) c.get(key);
		assertNull(readObject);
		
		//2
		c.set(key, 3600, "World");
		readObject = (String) c.get(key);

		assertNotNull(readObject);
		assertEquals("World",  readObject);
		
		Object client = c.getBaseClient();
		if(client instanceof net.rubyeye.xmemcached.MemcachedClient) {
			net.rubyeye.xmemcached.MemcachedClient xmc = (net.rubyeye.xmemcached.MemcachedClient) client;
			try {
				xmc.flushAllWithNoReply();
			} catch (InterruptedException ex) {
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			} catch (net.rubyeye.xmemcached.exception.MemcachedException ex) {
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			} 
			
			readObject = (String) c.get(key);
			assertNull(readObject);
		} else if (client instanceof net.spy.memcached.MemcachedClient) {			
			//does not support noreply
		}
		
	}
	
	public void testDoubleSet1() throws TimeoutException, MemcachedException {
		String readObject = null;
		String key = null;
		String value = null;
		
		//1
		key = "testdset1";
        value = "v1";
		c.set(key, 3600, value);
		readObject = (String) c.get(key);

		assertNotNull(readObject);
		assertEquals("v1",  readObject);
		
		value = "v2";
		c.set(key, 3600, value);
		readObject = (String) c.get(key);

		assertNotNull(readObject);
		assertEquals("v2",  readObject);

		//2
		key = "testdset2";
        Map valuemap = new HashMap();
		valuemap.put("key1", "v1");
		
		c.set(key, 3600, valuemap);
		Map readObjectMap = (Map) c.get(key);

		assertNotNull(readObjectMap);
		assertEquals(valuemap,  readObjectMap);
		
		valuemap.put("key2", "v2");
		c.set(key, 3600, valuemap);
		readObjectMap = (Map) c.get(key);

		assertNotNull(readObjectMap);
		assertEquals(valuemap,  readObjectMap);
		
		//3
		valuemap.put("key2", "v3");
		Object client = c.getBaseClient();
		if(client instanceof net.rubyeye.xmemcached.MemcachedClient) {
			net.rubyeye.xmemcached.MemcachedClient xmc = (net.rubyeye.xmemcached.MemcachedClient) client;
			try {
				xmc.setWithNoReply(key, 3600, valuemap);
			} catch (InterruptedException ex) {
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			} catch (net.rubyeye.xmemcached.exception.MemcachedException ex) {
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			} 
			
			readObjectMap = (Map) c.get(key);

			assertNotNull(readObjectMap);
			assertEquals(valuemap,  readObjectMap);
		} else if (client instanceof net.spy.memcached.MemcachedClient) {			
			//does not support noreply
		}
		
	}
	
	
	public void testBigKey250() throws TimeoutException, MemcachedException {
		String readObject = null;
		String key = null;
		
		StringBuilder keyPrefix = new StringBuilder();
		for(int i=0;i<249;i++) {
			keyPrefix.append("k");
		}
		
		//1 - String
		key = keyPrefix.toString()+"1";
		c.set(key, 3600, "World");
		readObject = (String) c.get(key);
		
		assertNotNull(readObject);
		assertEquals("World",  readObject);
    
		//2 - native obj
        Date value = new Date();
		key = keyPrefix.toString()+"2";
		c.set(key, 3600, value);
		Date readObjectDate = (Date) c.get(key);

		assertNotNull(readObjectDate);
		assertEquals(value.getTime(),  readObjectDate.getTime());
		
		//3 - custom obj
        TestObject testObject = new TestObject();
		testObject.setName(key);
		key = keyPrefix.toString()+"3";
		c.set(key, 3600, testObject);
		TestObject readTestObject = (TestObject) c.get(key);

		assertNotNull(readTestObject);
		assertEquals(testObject.getName(),  readTestObject.getName());
		
		//4 - no reply
		Object client = c.getBaseClient();
		key = keyPrefix.toString()+"4";
		if(client instanceof net.rubyeye.xmemcached.MemcachedClient) {
			net.rubyeye.xmemcached.MemcachedClient xmc = 
				(net.rubyeye.xmemcached.MemcachedClient) client;
			
			try {
				xmc.setWithNoReply(key, 3600, "World");			
			} catch (InterruptedException ex) {
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			} catch (net.rubyeye.xmemcached.exception.MemcachedException ex) {
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			} 			
			readObject = (String) c.get(key);
			assertNotNull(readObject);
			assertEquals("World",  readObject);
		} else if (client instanceof net.spy.memcached.MemcachedClient) {
			net.spy.memcached.MemcachedClient smc = (net.spy.memcached.MemcachedClient) client;
			
			//does not support noreply
		}		
	}
	
	public void testBigKey251() throws TimeoutException, MemcachedException {
		if(c instanceof QuickCachedClientImpl) {
			c.setMaxSizeAllowedForKey(-1);
		}
		
		String readObject = null;
		String key = null;
		
		StringBuilder keyPrefix = new StringBuilder();
		for(int i=0;i<250;i++) {
			keyPrefix.append("k");
		}
		
		//1 - String
		key = keyPrefix.toString()+"1";
		try {
			c.set(key, 3600, "World");
			
			assertTrue(
				"This should not have passed.. we should have got a exception"
				, false);
		} catch(Exception e) {
			assertTrue("We are good", true);
		}
		
    
		//2 - native obj
        Date value = new Date();
		key = keyPrefix.toString()+"2";
		try {
			c.set(key, 3600, value);
			assertTrue(
				"This should not have passed.. we should have got a exception"
				, false);
		} catch(Exception e) {
			assertTrue("We are good", true);
		}
		
		//3 - custom obj
        TestObject testObject = new TestObject();
		testObject.setName(key);
		key = keyPrefix.toString()+"3";
		try {
			c.set(key, 3600, testObject);
			assertTrue(
				"This should not have passed.. we should have got a exception"
				, false);
		} catch(Exception e) {
			assertTrue("We are good", true);
		}
		
		//4 - no reply
		Object client = c.getBaseClient();
		key = keyPrefix.toString()+"4";
		if(client instanceof net.rubyeye.xmemcached.MemcachedClient) {
			net.rubyeye.xmemcached.MemcachedClient xmc = 
				(net.rubyeye.xmemcached.MemcachedClient) client;
			
			try {
				xmc.setWithNoReply(key, 3600, "World");			
				
				assertTrue(
					"This should not have passed.. we should have got a exception"
					, false);
			} catch (InterruptedException ex) {
				Logger.getLogger(ProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
			} catch (Exception ex) {
				assertTrue("We are good", true);
			} 	
		} else if (client instanceof net.spy.memcached.MemcachedClient) {
			net.spy.memcached.MemcachedClient smc = 
				(net.spy.memcached.MemcachedClient) client;
			
			//does not support noreply
		}		
	}
	
	public void testBigData5MB() throws TimeoutException, MemcachedException {
		String readObject = null;
		String key = null;
		
		StringBuilder keyPrefix = new StringBuilder("testget");
		
		StringBuilder valuePrefix = new StringBuilder();
		
		int mb5 = 1024*1024*5;
		for(int i=0;i<mb5;i++) {
			valuePrefix.append("v");
		}
		
		//1 - String
		key = keyPrefix.toString()+"1";
		c.set(key, 3600,valuePrefix.toString());
		readObject = (String) c.get(key);
		
		assertNotNull(readObject);
		assertEquals(valuePrefix.toString(),  readObject);
  	
	}
	
	public void testBigData6MB() throws TimeoutException, MemcachedException {
		if(c instanceof QuickCachedClientImpl) {
			c.setMaxSizeAllowedForValue(-1);
		} else if(c instanceof XMemcachedImpl) {
			//does not send the command to server at all for large value
			//todo verify this
			return;
		} else if(c instanceof SpyMemcachedImpl) {
			//does not send the command to server at all for large value
			//todo verify this
			return;
		} 
		
		String readObject = null;
		String key = null;
		
		StringBuilder keyPrefix = new StringBuilder("testget");
		
		StringBuilder valuePrefix = new StringBuilder();
		
		int mb5 = 1024*1024*6;
		for(int i=0;i<mb5;i++) {
			valuePrefix.append("v");
		}
		
		//1 - String
		key = keyPrefix.toString()+"1";
		try {
			c.set(key, 3600, valuePrefix.toString());
			
			readObject = (String) c.get(key);
		
			assertNotNull(readObject);
			assertEquals(valuePrefix.toString(),  readObject);
			
			assertTrue(
				"This should not have passed.. we should have got a exception"
				, false);
		} catch(Exception e) {
			assertTrue("We are good", true);
		}
	}
}
