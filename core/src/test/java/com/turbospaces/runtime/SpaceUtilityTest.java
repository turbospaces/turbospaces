package com.turbospaces.runtime;

import java.util.concurrent.Callable;

import junit.framework.Assert;

import org.junit.Test;
import org.springframework.dao.IncorrectResultSizeDataAccessException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import com.google.common.base.Function;
import com.turbospaces.api.SpaceException;
import com.turbospaces.core.SpaceUtility;
import com.turbospaces.model.CacheStoreEntryWrapper;
import com.turbospaces.serialization.DecoratedKryo;

@SuppressWarnings("javadoc")
public class SpaceUtilityTest {

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
                                                                              throws Exception {
        Kryo spaceKryo = new DecoratedKryo();
        ObjectBuffer objectBuffer = new ObjectBuffer( spaceKryo );
        objectBuffer.readObjectData( new byte[] { 1, 2, 3 }, CacheStoreEntryWrapper.class );
    }
}
