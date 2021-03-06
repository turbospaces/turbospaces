package com.turbospaces.collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.beans.factory.InitializingBean;

import com.esotericsoftware.kryo.ObjectBuffer;
import com.esotericsoftware.minlog.Log;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import com.turbospaces.api.SpaceExpirationListener;
import com.turbospaces.core.CapacityMonitor;
import com.turbospaces.core.EffectiveMemoryManager;
import com.turbospaces.core.JVMUtil;
import com.turbospaces.core.UnsafeMemoryManager;
import com.turbospaces.model.BO;
import com.turbospaces.model.CacheStoreEntryWrapper;
import com.turbospaces.model.TestEntity1;
import com.turbospaces.offmemory.ByteArrayPointer;
import com.turbospaces.serialization.DecoratedKryo;
import com.turbospaces.serialization.PropertiesSerializer;

@SuppressWarnings({ "javadoc" })
@RunWith(Parameterized.class)
public class OffHeapHashMapTest {
    static EffectiveMemoryManager memoryManager;
    static ObjectBuffer objectBuffer;
    static BO bo;
    static DecoratedKryo decoratedKryo;
    static PropertiesSerializer propertySerializer;

    String key1, key2, key3;
    ByteArrayPointer p1, p2, p3;
    byte[] bytes1, bytes2, bytes3;
    TestEntity1 entity1, entity2, entity3, entity4;
    CacheStoreEntryWrapper cacheStoreEntryWrapper1, cacheStoreEntryWrapper2, cacheStoreEntryWrapper3;
    OffHeapHashSet heapHashMap;

    @Parameters
    public static List<Object[]> data()
                                       throws Exception {
        memoryManager = new UnsafeMemoryManager();
        bo = TestEntity1.getPersistentEntity();
        decoratedKryo = new DecoratedKryo();
        BO.registerPersistentClasses( decoratedKryo, bo.getOriginalPersistentEntity() );
        objectBuffer = new ObjectBuffer( decoratedKryo );
        propertySerializer = new PropertiesSerializer( decoratedKryo, bo );

        return Arrays.asList( new Object[][] {
                { new OffHeapLinearProbingSegment( memoryManager, 2, propertySerializer, MoreExecutors.sameThreadExecutor(), new CapacityMonitor( bo
                        .getCapacityRestriction(), null ) ) },
                { new OffHeapLinearProbingSet( memoryManager, bo.getCapacityRestriction(), null, propertySerializer, MoreExecutors
                        .sameThreadExecutor() ) } } );
    }

    @SuppressWarnings({ "rawtypes" })
    public OffHeapHashMapTest(final OffHeapHashSet set) throws Exception {
        heapHashMap = set;
        if ( heapHashMap instanceof InitializingBean )
            ( (InitializingBean) heapHashMap ).afterPropertiesSet();
        set.setExpirationListeners( new SpaceExpirationListener() {
            @Override
            public void handleNotification(final Object entity,
                                           final Object id,
                                           final Class persistentClass,
                                           final int originalTimeToLive) {
                Assert.assertNotNull( entity );
                Assert.assertTrue( persistentClass == TestEntity1.class );
                Assert.assertTrue( originalTimeToLive > 0 );

                Log.debug( String.format( "%s has been expired, %s-%s", entity, persistentClass.getSimpleName(), originalTimeToLive ) );
            }
        } );
    }

    @Before
    public void before()
                        throws Exception {
        entity1 = new TestEntity1();
        entity2 = new TestEntity1();
        entity3 = new TestEntity1();
        entity4 = new TestEntity1();

        entity1.afterPropertiesSet();
        entity2.afterPropertiesSet();
        entity3.afterPropertiesSet();
        entity4.afterPropertiesSet();

        key1 = entity1.getUniqueIdentifier();
        key2 = entity2.getUniqueIdentifier();
        key3 = entity3.getUniqueIdentifier();

        bytes1 = new ObjectBuffer( decoratedKryo ).writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ) );
        bytes2 = new ObjectBuffer( decoratedKryo ).writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, entity2 ) );
        bytes3 = new ObjectBuffer( decoratedKryo ).writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, entity3 ) );

        p1 = new ByteArrayPointer( memoryManager, bytes1, entity1, Integer.MAX_VALUE );
        p2 = new ByteArrayPointer( memoryManager, bytes2, entity2, Integer.MAX_VALUE );
        p3 = new ByteArrayPointer( memoryManager, bytes3, entity3, Integer.MAX_VALUE );

        cacheStoreEntryWrapper1 = CacheStoreEntryWrapper.writeValueOf( bo, entity1 );
        cacheStoreEntryWrapper2 = CacheStoreEntryWrapper.writeValueOf( bo, entity2 );
        cacheStoreEntryWrapper3 = CacheStoreEntryWrapper.writeValueOf( bo, entity3 );
    }

    @After
    public void after()
                       throws Exception {
        heapHashMap.size();
        heapHashMap.size();
        System.out.println( heapHashMap );
        heapHashMap.evictAll();
        if ( heapHashMap instanceof OffHeapLinearProbingSegment ) {
            assertThat( ( (OffHeapLinearProbingSegment) heapHashMap ).getCapacityMonitor().getItemsCount(), is( 0L ) );
            assertThat( ( (OffHeapLinearProbingSegment) heapHashMap ).getCapacityMonitor().getMemoryUsed(), is( 0L ) );
        }
    }

    @Test
    public void canStoreAndRetrieveRemove() {
        heapHashMap.put( key1, p1 );
        heapHashMap.put( key2, p2 );
        heapHashMap.put( key3, p3 );

        ByteArrayPointer object1 = heapHashMap.getAsPointer( key1 );
        ByteArrayPointer object2 = heapHashMap.getAsPointer( key2 );
        ByteArrayPointer object3 = heapHashMap.getAsPointer( key3 );

        assertThat( ByteArrayPointer.getEntityState( object1.dumpAndGetAddress(), memoryManager ), is( bytes1 ) );
        assertThat( ByteArrayPointer.getEntityState( object2.dumpAndGetAddress(), memoryManager ), is( bytes2 ) );
        assertThat( ByteArrayPointer.getEntityState( object3.dumpAndGetAddress(), memoryManager ), is( bytes3 ) );

        List<ByteArrayPointer> templateMatch1 = heapHashMap.match( cacheStoreEntryWrapper1 );
        List<ByteArrayPointer> templateMatch2 = heapHashMap.match( cacheStoreEntryWrapper2 );
        List<ByteArrayPointer> templateMatch3 = heapHashMap.match( cacheStoreEntryWrapper3 );

        assertThat( ByteArrayPointer.getEntityState( templateMatch1.iterator().next().dumpAndGetAddress(), memoryManager ), is( bytes1 ) );
        assertThat( ByteArrayPointer.getEntityState( templateMatch2.iterator().next().dumpAndGetAddress(), memoryManager ), is( bytes2 ) );
        assertThat( ByteArrayPointer.getEntityState( templateMatch3.iterator().next().dumpAndGetAddress(), memoryManager ), is( bytes3 ) );

        Assert.assertTrue( heapHashMap.contains( key1 ) );
        Assert.assertTrue( heapHashMap.contains( key2 ) );
        Assert.assertTrue( heapHashMap.contains( key3 ) );
        Assert.assertFalse( heapHashMap.contains( key1 + key2 + key3 ) );

        heapHashMap.remove( key1 );
        Assert.assertFalse( heapHashMap.contains( key1 ) );

        heapHashMap.remove( UUID.randomUUID().toString() );

        ByteArrayPointer p4 = new ByteArrayPointer( memoryManager, bytes2, entity2, Integer.MAX_VALUE );
        Object prev = heapHashMap.put( key2, p4 );
        assertThat( prev, is( notNullValue() ) );
        assertThat( heapHashMap.getAsSerializedData( key2 ).array(), is( bytes2 ) );
    }

    @Test
    public void canStoreRemoveUnderFor1000Entities()
                                                    throws Exception {
        TestEntity1[] arr = new TestEntity1[2000];
        for ( int i = 0; i < arr.length; i++ ) {
            arr[i] = new TestEntity1();
            arr[i].afterPropertiesSet();

            byte[] bytes = objectBuffer.writeObjectData( arr[i] );
            ByteArrayPointer p = new ByteArrayPointer( memoryManager, bytes, arr[i], Integer.MAX_VALUE );
            heapHashMap.put( arr[i].getUniqueIdentifier(), p );
        }

        for ( int i = 0; i < arr.length / 2; i++ ) {
            Assert.assertTrue( heapHashMap.contains( arr[i].getUniqueIdentifier() ) );
            heapHashMap.remove( arr[i].getUniqueIdentifier() );
            Assert.assertFalse( heapHashMap.contains( arr[i].getUniqueIdentifier() ) );
        }

        heapHashMap.evictAll();

        for ( TestEntity1 element : arr ) {
            byte[] bytes = objectBuffer.writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, element ) );
            ByteArrayPointer p = new ByteArrayPointer( memoryManager, bytes, element, Integer.MAX_VALUE );

            heapHashMap.put( element.getUniqueIdentifier(), p );
        }
        heapHashMap.evictAll();
    }

    @Test
    public void sizeConsistent() {
        TestEntity1[] arr = new TestEntity1[2000];
        for ( int i = 0; i < arr.length; i++ ) {
            arr[i] = new TestEntity1();
            arr[i].afterPropertiesSet();

            byte[] bytes = objectBuffer.writeObjectData( arr[i] );
            ByteArrayPointer p = new ByteArrayPointer( memoryManager, bytes, arr[i], Integer.MAX_VALUE );
            heapHashMap.put( arr[i].getUniqueIdentifier(), p );
            assertThat( heapHashMap.size(), is( i + 1 ) );
        }
        ImmutableCollection<?> entities1 = heapHashMap.toImmutableMap().values();
        ImmutableSet<?> entities2 = heapHashMap.toImmutableSet();

        Assert.assertEquals( entities1.size(), entities2.size() );

        for ( Object o : entities1 )
            Assert.assertTrue( entities2.contains( o ) );
        for ( Object o : entities2 )
            Assert.assertTrue( entities1.contains( o ) );

        for ( int i = 0; i < arr.length; i++ ) {
            Assert.assertTrue( heapHashMap.contains( arr[i].getUniqueIdentifier() ) );
            heapHashMap.remove( arr[i].getUniqueIdentifier() );
            Assert.assertFalse( heapHashMap.contains( arr[i].getUniqueIdentifier() ) );
            assertThat( heapHashMap.size() + i + 1, is( arr.length ) );
        }
    }

    @Test
    public void canAddUpToNAndRemoveOneByOne() {
        final TestEntity1[] arr = new TestEntity1[OffHeapLinearProbingSegment.MAX_SEGMENT_CAPACITY * 4];
        Assert.assertTrue( JVMUtil.repeatConcurrently( Runtime.getRuntime().availableProcessors(), arr.length, new Function<Integer, Object>() {

            @Override
            public Object apply(final Integer i) {

                arr[i] = new TestEntity1();
                arr[i].afterPropertiesSet();

                ObjectBuffer b = new ObjectBuffer( decoratedKryo );
                byte[] bytes = b.writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, arr[i] ) );
                ByteArrayPointer p = new ByteArrayPointer( memoryManager, bytes, arr[i], Integer.MAX_VALUE );

                heapHashMap.put( arr[i].getUniqueIdentifier(), p );

                return null;
            }
        } ).size() == 0 );

        System.out.println( heapHashMap );
        Assert.assertTrue( JVMUtil.repeatConcurrently( Runtime.getRuntime().availableProcessors(), arr.length, new Function<Integer, Object>() {
            @Override
            public Object apply(final Integer i) {
                heapHashMap.remove( arr[i].getUniqueIdentifier() );
                return null;
            }
        } ).size() == 0 );
    }

    @Test
    public void canRemoveExpiredEntitiesAutomatically()
                                                       throws InterruptedException {
        TestEntity1[] arr = new TestEntity1[3000];
        for ( int i = 0; i < arr.length; i++ ) {
            arr[i] = new TestEntity1();
            arr[i].afterPropertiesSet();

            byte[] bytes = objectBuffer.writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, arr[i] ) );
            ByteArrayPointer p = new ByteArrayPointer( memoryManager, bytes, arr[i], 1 );

            heapHashMap.put( arr[i].getUniqueIdentifier(), p );
        }
        Thread.sleep( 2 );

        for ( int i = 0; i < arr.length / 3; i++ )
            Assert.assertTrue( heapHashMap.getAsPointer( arr[i].getUniqueIdentifier() ) == null );

        for ( int i = arr.length / 3; i < 2 * arr.length / 3; i++ )
            Assert.assertFalse( heapHashMap.contains( arr[i].getUniqueIdentifier() ) );

        for ( int i = 2 * arr.length / 3; i < arr.length; i++ )
            Assert.assertTrue( heapHashMap.match( CacheStoreEntryWrapper.writeValueOf( bo, arr[i] ) ) == null );
    }

    @Test
    public void canRemoveExpiredEntitiesAutomaticallyAfterCleanupOperationCall()
                                                                                throws InterruptedException {
        TestEntity1[] arr = new TestEntity1[1500];
        for ( int i = 0; i < arr.length; i++ ) {
            arr[i] = new TestEntity1();
            arr[i].afterPropertiesSet();

            byte[] bytes = objectBuffer.writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, arr[i] ) );
            ByteArrayPointer p = new ByteArrayPointer( memoryManager, bytes, arr[i], 1 );

            heapHashMap.put( arr[i].getUniqueIdentifier(), p );
        }
        Thread.sleep( 2 );
        heapHashMap.cleanUp();
        assertThat( heapHashMap.size(), is( 0 ) );
        for ( TestEntity1 element : arr )
            Assert.assertTrue( heapHashMap.getAsPointer( element.getUniqueIdentifier() ) == null );
    }
}
