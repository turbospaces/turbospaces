package com.turbospaces.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import com.google.common.base.Function;
import com.turbospaces.pool.ObjectPool;

@SuppressWarnings("javadoc")
public class JVMUtilTest {

    @BeforeClass
    public static void beforeClass() {
        JVMUtil.gcOnExit();
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
            public void run() {
                ObjectPool<ObjectBuffer> objectPool = JVMUtil.newObjectBufferPool();
                ObjectBuffer borrowObject = objectPool.borrowObject();
                borrowObject.setKryo( new Kryo() );
                objectPool.returnObject( borrowObject );
            }
        } );
    }

    @Test
    public void equals() {
        Assert.assertFalse( JVMUtil.equals( new Object(), new Object() ) );
        Assert.assertFalse( JVMUtil.equals( null, new Object() ) );
        Assert.assertFalse( JVMUtil.equals( new Object(), null ) );
        Assert.assertTrue( JVMUtil.equals( Integer.valueOf( 1 ), Integer.valueOf( 1 ) ) );
        Assert.assertTrue( JVMUtil.equals( Integer.valueOf( 1 ), new Integer( 1 ) ) );
        Assert.assertTrue( JVMUtil.equals( new int[] { 1, 2 }, new int[] { 1, 2 } ) );
        Assert.assertFalse( JVMUtil.equals( new int[] { 1, 2 }, new int[] { 1, 2, 3 } ) );
        Assert.assertTrue( JVMUtil.equals( new long[] { 1, 2 }, new long[] { 1, 2 } ) );
        Assert.assertFalse( JVMUtil.equals( new long[] { 1, 2 }, new long[] { 1, 2, 3 } ) );
        Assert.assertTrue( JVMUtil.equals( new short[] { 1, 2 }, new short[] { 1, 2 } ) );
        Assert.assertFalse( JVMUtil.equals( new short[] { 1, 2 }, new short[] { 1, 2, 3 } ) );
        Assert.assertTrue( JVMUtil.equals( new byte[] { 1, 2 }, new byte[] { 1, 2 } ) );
        Assert.assertFalse( JVMUtil.equals( new byte[] { 1, 2 }, new byte[] { 1, 2, 3 } ) );
        Assert.assertTrue( JVMUtil.equals( new char[] { 1, 2 }, new char[] { 1, 2 } ) );
        Assert.assertFalse( JVMUtil.equals( new char[] { 1, 2 }, new char[] { 1, 2, 3 } ) );
        Assert.assertTrue( JVMUtil.equals( new float[] { 1, 2 }, new float[] { 1, 2 } ) );
        Assert.assertFalse( JVMUtil.equals( new float[] { 1, 2 }, new float[] { 1, 2, 3 } ) );
        Assert.assertTrue( JVMUtil.equals( new double[] { 1, 2 }, new double[] { 1, 2 } ) );
        Assert.assertFalse( JVMUtil.equals( new double[] { 1, 2 }, new double[] { 1, 2, 3 } ) );
        Assert.assertTrue( JVMUtil.equals( new boolean[] { true, false }, new boolean[] { true, false } ) );
        Assert.assertFalse( JVMUtil.equals( new boolean[] { true, true }, new boolean[] { true, false } ) );
    }
}
