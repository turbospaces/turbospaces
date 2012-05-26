package com.turbospaces.api;

import junit.framework.Assert;

import org.junit.Test;

import com.turbospaces.core.Memory;

@SuppressWarnings("javadoc")
public class CapacityRestrictionTest {

    @Test(expected = IllegalArgumentException.class)
    public void canGetExceptionForUnproperLRUConfiguration1() {
        CapacityRestriction restriction = new CapacityRestriction();
        restriction.setEvictionPolicy( CacheEvictionPolicy.LRU );
        restriction.setEvictionPercentage( 0 );
    }

    @Test(expected = IllegalArgumentException.class)
    public void canGetExceptionForUnproperLRUConfiguration2() {
        CapacityRestriction restriction = new CapacityRestriction();
        restriction.setEvictionPolicy( CacheEvictionPolicy.LRU );
        restriction.setEvictionPercentage( 101 );
    }

    @Test(expected = IllegalStateException.class)
    public void canGetExceptionForUnproperLRUConfiguration3() {
        CapacityRestriction restriction = new CapacityRestriction();
        restriction.setEvictionPercentage( 56 );
    }

    @Test
    public void sunnyDayScenario() {
        CapacityRestriction restriction = new CapacityRestriction();
        restriction.setEvictionPolicy( CacheEvictionPolicy.LRU );
        restriction.setEvictionPercentage( 55 );
        restriction.setMaxElements( 123 );
        restriction.setMaxMemorySizeInMb( 1 );
        CapacityRestriction clone = restriction.clone();

        Assert.assertEquals( CacheEvictionPolicy.LRU, clone.getEvictionPolicy() );
        Assert.assertEquals( 55, clone.getEvictionPercentage() );
        Assert.assertEquals( 123, clone.getMaxElements() );
        Assert.assertEquals( Memory.mb( 1 ), clone.getMaxMemorySizeInBytes() );
    }
}
