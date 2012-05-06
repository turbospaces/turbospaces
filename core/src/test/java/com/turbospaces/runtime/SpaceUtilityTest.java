package com.turbospaces.runtime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.google.common.base.Function;
import com.turbospaces.api.SpaceException;
import com.turbospaces.core.SpaceUtility;

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
}
