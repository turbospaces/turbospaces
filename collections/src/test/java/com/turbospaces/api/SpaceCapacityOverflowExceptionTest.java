package com.turbospaces.api;

import junit.framework.Assert;

import org.junit.Test;

import com.turbospaces.model.TestEntity1;

@SuppressWarnings("javadoc")
public class SpaceCapacityOverflowExceptionTest {

    @Test
    public void test() {
        SpaceCapacityOverflowException e = new SpaceCapacityOverflowException( 127, new TestEntity1() );
        e.fillInStackTrace();
        System.out.println( e.toString() );
        Assert.assertEquals( 127, e.getMaxElements() );
        Assert.assertEquals( TestEntity1.class, e.getObj().getClass() );
    }
}
