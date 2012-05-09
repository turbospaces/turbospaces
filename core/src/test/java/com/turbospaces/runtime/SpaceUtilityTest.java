package com.turbospaces.runtime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.junit.Test;
import org.springframework.dao.IncorrectResultSizeDataAccessException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import com.google.common.base.Function;
import com.turbospaces.api.SpaceException;
import com.turbospaces.core.SpaceUtility;
import com.turbospaces.model.TestEntity1;
import com.turbospaces.spaces.CacheStoreEntryWrapper;

@SuppressWarnings("javadoc")
public class SpaceUtilityTest {

    @Test
    public void canAllocateMemory() {
        byte[] arr = new byte[] { 1, 2, 3, 4, 5 };
        long address1 = SpaceUtility.allocateMemory( arr.length );
        SpaceUtility.writeBytesArray( address1, arr );
        byte[] array = SpaceUtility.readBytesArray( address1, arr.length );
        assertThat( array, is( arr ) );
    }

    @Test
    public void allocate() {
        for ( int i = 0; i < 100; i++ ) {
            long address = SpaceUtility.allocateMemory( 280 );
            SpaceUtility.releaseMemory( address );
        }
    }

    @Test
    public void canReallocate() {
        byte[] arr = new byte[] { 1, 2, 3, 4, 5 };
        long address = SpaceUtility.allocateMemory( arr.length );
        SpaceUtility.writeBytesArray( address, arr );
        arr = new byte[] { 1, 2, 3, 4, 5, 6 };
        address = SpaceUtility.reallocate( address, arr.length );
        byte[] array = SpaceUtility.readBytesArray( address, arr.length );
        assertThat( array.length, is( 6 ) );
    }

    @Test
    public void repeateConcurrentlyWorks()
                                          throws InterruptedException {
        final AtomicInteger integer = new AtomicInteger();
        SpaceUtility.repeatConcurrently( 2, 5, new Function<Integer, Object>() {

            @Override
            public Object apply(final Integer input) {
                integer.incrementAndGet();
                return this;
            }
        } );
        assertThat( integer.get(), is( 5 ) );
    }

    @Test(expected = SpaceException.class)
    public void canGetSpaceExceptionWhenExceptionIsNotExpected() {
        SpaceUtility.exceptionShouldNotHappen( new Callable<Object>() {

            @Override
            public Object call()
                                throws Exception {
                SpaceUtility.raiseSpaceCapacityOverflowException( Long.MAX_VALUE, new Object() );
                return new Object();
            }
        } );
    }

    @Test
    public void matchesByTemplateProperly() {
        Object[] arr = new Object[] { "1", 2 };
        Assert.assertTrue( SpaceUtility.macthesByPropertyValues( new Object[] { null, 2 }, arr ) );
        Assert.assertTrue( SpaceUtility.macthesByPropertyValues( new Object[] { "1", null }, arr ) );
        Assert.assertFalse( SpaceUtility.macthesByPropertyValues( new Object[] { "2", 2 }, arr ) );
        Assert.assertFalse( SpaceUtility.macthesByPropertyValues( new Object[] { "1", 3 }, arr ) );
    }

    @Test
    public void canGetPublicInformation() {
        System.out.println( SpaceUtility.projecBuildTimestamp() );
        System.out.println( SpaceUtility.projectVersion() );
    }

    @Test
    public void canGetSingeResult() {
        Assert.assertTrue( SpaceUtility.singleResult( new Object[] { "1" } ).isPresent() );
    }

    @Test(expected = IncorrectResultSizeDataAccessException.class)
    public void canGetExceptionForNonSingeResult() {
        SpaceUtility.singleResult( new Object[] { "1", "2" } );
    }

    @Test(expected = SpaceException.class)
    public void handlesUnexceptedException() {
        SpaceUtility.exceptionShouldNotHappen( new Function<Integer, String>() {

            @Override
            public String apply(final Integer input) {
                throw new IllegalArgumentException( input.toString() );
            }
        }, 1 );
    }

    @Test(expected = UnsupportedOperationException.class)
    public void canGetUnsupportedOperationExceptionForCacheStoreEntryWrapper()
                                                                              throws ClassNotFoundException,
                                                                              Exception {
        Kryo spaceKryo = SpaceUtility.spaceKryo( TestEntity1.configurationFor(), null );
        ObjectBuffer objectBuffer = new ObjectBuffer( spaceKryo );
        objectBuffer.readObjectData( new byte[] { 1, 2, 3 }, CacheStoreEntryWrapper.class );
    }
}
