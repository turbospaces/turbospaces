package com.elsecloud.core;

import junit.framework.Assert;

import org.jgroups.util.Util;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("javadoc")
public class RuntimeUtilityTest {

    @BeforeClass
    public static void beforeClass() {
        SpaceUtility.gcOnExit();
    }

    @Test
    public void testDumpActiveThreads() {
        Util.dumpThreads();
    }

    @Test
    public void canDoGC() {
        for ( int i = 0; i < 10000; i++ )
            new Object().toString();

        SpaceUtility.gc();
    }

    @Test
    public void canDealWithDeadThread()
                                       throws InterruptedException {
        Thread t = new Thread();
        t.start();
        t.join();

        Assert.assertFalse( t.isAlive() );

        System.err.println( Util.dumpThreads() );
    }
}
