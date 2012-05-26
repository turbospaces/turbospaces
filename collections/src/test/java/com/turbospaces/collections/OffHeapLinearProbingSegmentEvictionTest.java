package com.turbospaces.collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.esotericsoftware.kryo.ObjectBuffer;
import com.google.common.util.concurrent.MoreExecutors;
import com.turbospaces.api.CacheEvictionPolicy;
import com.turbospaces.model.BO;
import com.turbospaces.model.CacheStoreEntryWrapper;
import com.turbospaces.model.TestEntity1;
import com.turbospaces.offmemory.ByteArrayPointer;
import com.turbospaces.serialization.DecoratedKryo;
import com.turbospaces.serialization.PropertiesSerializer;

@SuppressWarnings("javadoc")
public class OffHeapLinearProbingSegmentEvictionTest {
    BO bo;
    DecoratedKryo decoratedKryo;
    ObjectBuffer objectBuffer;
    OffHeapLinearProbingSegment segment;
    PropertiesSerializer propertySerializer;

    @Before
    public void setUp()
                       throws Exception {
        bo = TestEntity1.getPersistentEntity();
        decoratedKryo = new DecoratedKryo();
        BO.registerPersistentClasses( decoratedKryo, bo.getOriginalPersistentEntity() );
        propertySerializer = new PropertiesSerializer( decoratedKryo, bo );
        objectBuffer = new ObjectBuffer( decoratedKryo );
    }

    @After
    public void tearDown() {
        segment.destroy();
    }

    @Test(expected = IllegalStateException.class)
    public void negativeScenario() {
        segment = new OffHeapLinearProbingSegment( 2, propertySerializer, MoreExecutors.sameThreadExecutor(), CacheEvictionPolicy.REJECT );
        segment.evictElements( 2 );
    }

    @Test
    public void testRandomEviction() {
        int size = 12;
        segment = new OffHeapLinearProbingSegment( size, propertySerializer, MoreExecutors.sameThreadExecutor(), CacheEvictionPolicy.RANDOM );

        for ( int i = 0; i < size; i++ ) {
            TestEntity1 e = new TestEntity1();
            e.afterPropertiesSet();

            byte[] bytes = objectBuffer.writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, e ) );
            ByteArrayPointer p = new ByteArrayPointer( bytes, e, Integer.MAX_VALUE );

            segment.put( e.getUniqueIdentifier(), p );
        }

        // 25 % = 3
        assertThat( segment.evictPercentage( 25 ), is( 3 ) );
        assertThat( segment.evictElements( 2 ), is( 2 ) );

        // 7?
        assertThat( segment.size(), is( 7 ) );
    }

    @Test
    public void testRandomizeEvictionAfterExpiration()
                                                      throws InterruptedException {
        segment = new OffHeapLinearProbingSegment( 2, propertySerializer, MoreExecutors.sameThreadExecutor(), CacheEvictionPolicy.RANDOM );

        TestEntity1 e = new TestEntity1();
        e.afterPropertiesSet();

        byte[] bytes = objectBuffer.writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, e ) );
        ByteArrayPointer p = new ByteArrayPointer( bytes, e, 1 );

        segment.put( e.getUniqueIdentifier(), p );
        Thread.sleep( 1 );
        assertThat( segment.evictPercentage( 25 ), is( 0 ) );
    }

    @Test
    public void testLruEviction() {

    }
}
