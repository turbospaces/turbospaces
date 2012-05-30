package com.turbospaces.runtime;

import junit.framework.Assert;

import org.junit.Test;
import org.springframework.dao.IncorrectResultSizeDataAccessException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import com.turbospaces.core.SpaceUtility;
import com.turbospaces.model.CacheStoreEntryWrapper;
import com.turbospaces.serialization.DecoratedKryo;

@SuppressWarnings("javadoc")
public class SpaceUtilityTest {

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

    @Test(expected = UnsupportedOperationException.class)
    public void canGetUnsupportedOperationExceptionForCacheStoreEntryWrapper()
                                                                              throws Exception {
        Kryo spaceKryo = new DecoratedKryo();
        ObjectBuffer objectBuffer = new ObjectBuffer( spaceKryo );
        objectBuffer.readObjectData( new byte[] { 1, 2, 3 }, CacheStoreEntryWrapper.class );
    }
}
