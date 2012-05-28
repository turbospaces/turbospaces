package com.turbospaces.model;

import junit.framework.Assert;

import org.junit.Test;

@SuppressWarnings("javadoc")
public class ExplicitCacheEntryTest {

    @Test
    public void coverage() {
        TestEntity1 entity1 = new TestEntity1();
        TestEntity1 entity2 = new TestEntity1();
        entity1.afterPropertiesSet();
        entity2.afterPropertiesSet();
        ExplicitCacheEntry<String, TestEntity1> e1 = new ExplicitCacheEntry<String, TestEntity1>( entity1.getUniqueIdentifier(), entity1 );
        ExplicitCacheEntry<String, TestEntity1> e2 = new ExplicitCacheEntry<String, TestEntity1>( entity2.getUniqueIdentifier(), entity2 );

        Assert.assertEquals( e1.toString(), e1.toString() );
        Assert.assertFalse( e1.toString().equals( e2.toString() ) );
        Assert.assertFalse( e1.toString().equals( new Object().toString() ) );

        Assert.assertEquals( e1.hashCode(), e1.hashCode() );
        Assert.assertFalse( e1.hashCode() == e2.hashCode() );
        Assert.assertFalse( e1.equals( e2 ) );
        Assert.assertFalse( e1.equals( new Object() ) );
    }
}
