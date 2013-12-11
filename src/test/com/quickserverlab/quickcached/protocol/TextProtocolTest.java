package com.quickserverlab.quickcached.protocol;

import java.io.IOException;

import java.util.logging.*;
import com.quickserverlab.quickcached.client.MemcachedClient;


/**
 * JUnit test cases for QuickCached
 */
public class TextProtocolTest extends ProtocolTest {

    public TextProtocolTest(String name) {
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
        junit.textui.TestRunner.run(TextProtocolTest.class);
    }
	
}
