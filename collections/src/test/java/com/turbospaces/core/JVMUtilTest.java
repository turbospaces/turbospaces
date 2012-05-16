package com.turbospaces.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

@SuppressWarnings("javadoc")
public class JVMUtilTest {

    @BeforeClass
    public static void beforeClass() {
        JVMUtil.gcOnExit();
    }

    @Test
    public void canAllocateMemory() {
        byte[] arr = new byte[] { 1, 2, 3, 4, 5 };
        long address1 = JVMUtil.allocateMemory( arr.length );
        JVMUtil.writeBytesArray( address1, arr );
        byte[] array = JVMUtil.readBytesArray( address1, arr.length );
        assertThat( array, is( arr ) );

        long longMemory = JVMUtil.allocateMemory( Longs.BYTES );
        JVMUtil.putLong( longMemory, System.currentTimeMillis() );
        assertThat( JVMUtil.getLong( longMemory ), is( lessThanOrEqualTo( System.currentTimeMillis() ) ) );
        JVMUtil.releaseMemory( longMemory );

        long intMemory = JVMUtil.allocateMemory( Ints.BYTES );
        JVMUtil.putInt( intMemory, Integer.MAX_VALUE );
        assertThat( JVMUtil.getInt( intMemory ), is( Integer.MAX_VALUE ) );
        JVMUtil.releaseMemory( intMemory );
    }

    @Test
    public void allocate() {
        for ( int i = 0; i < 100; i++ ) {
            long address = JVMUtil.allocateMemory( 280 );
            JVMUtil.releaseMemory( address );
        }
    }

    @Test
    public void canReallocate() {
        byte[] arr = new byte[] { 1, 2, 3, 4, 5 };
        long address = JVMUtil.allocateMemory( arr.length );
        JVMUtil.writeBytesArray( address, arr );
        arr = new byte[] { 1, 2, 3, 4, 5, 6 };
        address = JVMUtil.reallocate( address, arr.length );
        byte[] array = JVMUtil.readBytesArray( address, arr.length );
        assertThat( array.length, is( 6 ) );
    }

    @Test
    public void repeateConcurrentlyWorks() {
        final AtomicInteger integer = new AtomicInteger();
        JVMUtil.repeatConcurrently( 2, 5, new Function<Integer, Object>() {
            @Override
            public Object apply(final Integer input) {
                integer.incrementAndGet();
                return this;
            }
        } );
        assertThat( integer.get(), is( 5 ) );
    }

    @Test
    public void canGetExceptionsConcurenly() {
        assertThat( JVMUtil.repeatConcurrently( Runtime.getRuntime().availableProcessors(), 100, new Runnable() {
            AtomicInteger counter = new AtomicInteger();

            @Override
            public void run() {
                if ( counter.incrementAndGet() % 2 == 0 )
                    throw new IllegalStateException( "exception" );
            }
        } ).size(), is( greaterThan( 0 ) ) );
    }

    @Test
    public void canRunAndGetException() {
        assertThat( JVMUtil.runAndGetExecutionException( new Runnable() {
            @Override
            public void run() {
                throw new NullPointerException();
            }
        } ), is( notNullValue() ) );
    }

    @Test(expected = AssertionError.class)
    public void canRunAndDidntGetException() {
        JVMUtil.runAndGetExecutionException( new Runnable() {
            @Override
            public void run() {}

        } );
    }
}
