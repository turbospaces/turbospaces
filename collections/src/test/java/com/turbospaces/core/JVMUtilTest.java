package com.turbospaces.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import com.google.common.base.Function;
import com.lmax.disruptor.util.Util;
import com.turbospaces.model.TestEntity1;

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
                ObjectBuffer borrowObject = new ObjectBuffer( new Kryo() );
                borrowObject.setKryo( new Kryo() );
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

    @Test
    public void canExtractPropertyValuesUnderUnsafe() {
        for ( int i = 0; i < 1000; i++ ) {
            TestEntity1 entity1 = new TestEntity1();
            entity1.afterPropertiesSet();

            long fi1Offset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "fi1" ) );
            long fi2Offset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "fi2" ) );
            long b1Offset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "b1" ) );
            long b2Offset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "b2" ) );
            long l1Offset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "l1" ) );
            long l2Offset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "l2" ) );
            long s1Offset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "sh1" ) );
            long s2Offset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "sh2" ) );
            long d1Offset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "dp1" ) );
            long d2Offset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "dp2" ) );
            long f1Offset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "fp1" ) );
            long f2Offset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "fp2" ) );
            long c1Offset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "cp1" ) );
            long c2Offset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "cp2" ) );
            long lp1Offset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "lp1" ) );
            long lp2Offset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "lp2" ) );
            long longsOffset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "longs" ) );

            Assert.assertEquals( entity1.fi1, (int) JVMUtil.getPropertyValueUnsafe( entity1, int.class, fi1Offset ) );
            Assert.assertEquals( entity1.fi2, (int) JVMUtil.getPropertyValueUnsafe( entity1, int.class, fi2Offset ) );
            Assert.assertEquals( entity1.l1, JVMUtil.getPropertyValueUnsafe( entity1, Long.class, l1Offset ) );
            Assert.assertEquals( entity1.l2, JVMUtil.getPropertyValueUnsafe( entity1, Long.class, l2Offset ) );
            Assert.assertEquals( entity1.b1, (boolean) JVMUtil.getPropertyValueUnsafe( entity1, boolean.class, b1Offset ) );
            Assert.assertEquals( entity1.b2, (boolean) JVMUtil.getPropertyValueUnsafe( entity1, boolean.class, b2Offset ) );
            Assert.assertEquals( entity1.sh1, (short) JVMUtil.getPropertyValueUnsafe( entity1, short.class, s1Offset ) );
            Assert.assertEquals( entity1.sh2, (short) JVMUtil.getPropertyValueUnsafe( entity1, short.class, s2Offset ) );
            Assert.assertEquals( entity1.dp1, JVMUtil.getPropertyValueUnsafe( entity1, double.class, d1Offset ) );
            Assert.assertEquals( entity1.dp2, JVMUtil.getPropertyValueUnsafe( entity1, double.class, d2Offset ) );
            Assert.assertEquals( entity1.fp1, JVMUtil.getPropertyValueUnsafe( entity1, float.class, f1Offset ) );
            Assert.assertEquals( entity1.fp2, JVMUtil.getPropertyValueUnsafe( entity1, float.class, f2Offset ) );
            Assert.assertEquals( entity1.cp1, (char) JVMUtil.getPropertyValueUnsafe( entity1, char.class, c1Offset ) );
            Assert.assertEquals( entity1.cp2, (char) JVMUtil.getPropertyValueUnsafe( entity1, char.class, c2Offset ) );
            Assert.assertEquals( entity1.lp1, (long) JVMUtil.getPropertyValueUnsafe( entity1, long.class, lp1Offset ) );
            Assert.assertEquals( entity1.lp2, (long) JVMUtil.getPropertyValueUnsafe( entity1, long.class, lp2Offset ) );
            Assert.assertEquals( entity1.longs, JVMUtil.getPropertyValueUnsafe( entity1, ArrayList.class, longsOffset ) );
        }
    }

    @Test
    public void canSetPropertyValuesUnderUnsafe() {
        for ( int i = 0; i < 1000; i++ ) {
            TestEntity1 entity1 = new TestEntity1();
            entity1.afterPropertiesSet();

            long fi1Offset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "fi1" ) );
            long b1Offset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "b1" ) );
            long l1Offset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "l1" ) );
            long s1Offset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "sh1" ) );
            long d1Offset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "dp1" ) );
            long f1Offset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "fp1" ) );
            long c1Offset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "cp1" ) );
            long lp1Offset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "lp1" ) );
            long longsOffset = Util.getUnsafe().objectFieldOffset( JVMUtil.fieldFor( TestEntity1.class, "longs" ) );

            TestEntity1 clone = new TestEntity1();

            JVMUtil.setPropertyValueUnsafe( clone, entity1.fi1, int.class, fi1Offset );
            JVMUtil.setPropertyValueUnsafe( clone, entity1.l1, Long.class, l1Offset );
            JVMUtil.setPropertyValueUnsafe( clone, entity1.b1, boolean.class, b1Offset );
            JVMUtil.setPropertyValueUnsafe( clone, entity1.sh1, short.class, s1Offset );
            JVMUtil.setPropertyValueUnsafe( clone, entity1.dp1, double.class, d1Offset );
            JVMUtil.setPropertyValueUnsafe( clone, entity1.fp1, float.class, f1Offset );
            JVMUtil.setPropertyValueUnsafe( clone, entity1.cp1, char.class, c1Offset );
            JVMUtil.setPropertyValueUnsafe( clone, entity1.lp1, long.class, lp1Offset );
            JVMUtil.setPropertyValueUnsafe( clone, entity1.longs, ArrayList.class, longsOffset );

            Assert.assertEquals( entity1.fi1, clone.fi1 );
            Assert.assertEquals( entity1.l1, clone.l1 );
            Assert.assertEquals( entity1.b1, clone.b1 );
            Assert.assertEquals( entity1.sh1, clone.sh1 );
            Assert.assertEquals( entity1.dp1, clone.dp1 );
            Assert.assertEquals( entity1.fp1, clone.fp1 );
            Assert.assertEquals( entity1.cp1, clone.cp1 );
            Assert.assertEquals( entity1.lp1, clone.lp1 );
            Assert.assertEquals( entity1.longs, clone.longs );
        }
    }
}
