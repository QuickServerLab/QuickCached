package com.quickserverlab.quickcached;

import com.quickserverlab.quickcached.protocol.BinaryProtocolTest;
import com.quickserverlab.quickcached.protocol.TextProtocolTest;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;


/**
 * Class to build  TestSuite out of the individual test classes.
 */
public class AllJUnitTests extends TestCase {

    public AllJUnitTests(String name) {
        super(name);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(SkeletonTest.class);

		//suite.addTest(new TestSuite(BasicTest.class));
        suite.addTest(new TestSuite(TextProtocolTest.class));
		suite.addTest(new TestSuite(BinaryProtocolTest.class));
		//suite.addTest(new TestSuite(UDPTest.class));
        return suite;
   }
}
