package com.elsecloud.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("javadoc")
public class CacheStatisticsTest {
    CacheStatistics cacheStatistics;

    @Before
    public void setUp() {
        cacheStatistics = new CacheStatistics();
    }

    @Test
    public void test() {
        cacheStatistics.increaseExclusiveReadsCount();
        cacheStatistics.increaseHitsCount();
        cacheStatistics.increasePutsCount();
        cacheStatistics.increaseTakesCount();
        cacheStatistics.setOffHeapBytesOccupied( 1 );

        CacheStatistics clone = cacheStatistics.clone();
        cacheStatistics.reset();

        assertThat( clone.getExclusiveReadsCount(), is( 1L ) );
        assertThat( clone.getHitsCount(), is( 1L ) );
        assertThat( clone.getOffHeapBytesOccupied(), is( 1L ) );
        assertThat( clone.getPutsCount(), is( 1L ) );
        assertThat( clone.getTakesCount(), is( 1L ) );

        System.out.println( clone );
    }
}
