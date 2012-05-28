package com.turbospaces.core;

import java.util.Random;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.turbospaces.core.CacheStatisticsCounter.CompleteCacheStats;

@SuppressWarnings("javadoc")
public class CacheStatisticsCounterTest {
    CacheStatisticsCounter csc;

    @Before
    public void setUp()
                       throws Exception {
        csc = new CacheStatisticsCounter();
    }

    @After
    public void tearDown()
                          throws Exception {
        System.out.println( csc.snapshotCompleteCacheStats() );
        System.out.println( csc.toString() );
    }

    @Test
    public void test() {
        Random r = new Random();
        long puts = Math.max( 1, r.nextInt() );
        long takes = Math.max( 1, r.nextInt() );
        long exclusiveReads = Math.max( 1, r.nextInt() );
        long hits = Math.max( 1, r.nextInt() );
        long loadException = System.currentTimeMillis();
        long loadSuccesss = System.currentTimeMillis();
        long misses = Math.max( 1, r.nextInt() );

        csc.recordPuts( (int) puts );
        csc.recordTakes( (int) takes );
        csc.recordExclusiveReads( (int) exclusiveReads );

        csc.recordHits( (int) hits );
        csc.recordLoadException( loadException );
        csc.recordLoadSuccess( loadSuccesss );
        csc.recordMisses( (int) misses );
        csc.recordEviction();

        CompleteCacheStats completeCacheStats = csc.snapshotCompleteCacheStats();
        Assert.assertEquals( completeCacheStats.exclusiveReadsCount(), exclusiveReads );
        Assert.assertEquals( completeCacheStats.putsCount(), puts );
        Assert.assertEquals( completeCacheStats.takesCount(), takes );
        Assert.assertEquals( completeCacheStats.readStats().hitCount(), hits );
        Assert.assertEquals( completeCacheStats.readStats().missCount(), misses );
    }
}
