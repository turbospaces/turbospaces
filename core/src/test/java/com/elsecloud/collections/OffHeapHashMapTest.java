package com.elsecloud.collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mapping.model.BasicPersistentEntity;

import com.elsecloud.api.SpaceConfiguration;
import com.elsecloud.api.SpaceExpirationListener;
import com.elsecloud.core.SpaceUtility;
import com.elsecloud.model.BO;
import com.elsecloud.model.TestEntity1;
import com.elsecloud.offmemory.ByteArrayPointer;
import com.elsecloud.serialization.DefaultEntitySerializer;
import com.elsecloud.spaces.CacheStoreEntryWrapper;
import com.esotericsoftware.kryo.ObjectBuffer;

@SuppressWarnings({ "javadoc", "rawtypes" })
@RunWith(Parameterized.class)
public class OffHeapHashMapTest {
    static Logger LOGGER = LoggerFactory.getLogger( OffHeapHashMapTest.class );
    static SpaceConfiguration configuration;

    static DefaultEntitySerializer serializer;
    static ObjectBuffer objectBuffer;
    static BO bo;

    String key1, key2, key3;
    ByteArrayPointer p1, p2, p3;
    byte[] bytes1, bytes2, bytes3;
    TestEntity1 entity1, entity2, entity3, entity4;
    OffHeapHashSet heapHashMap;

    @SuppressWarnings("unchecked")
    @Parameters
    public static List<Object[]> data()
                                       throws Exception {
        configuration = TestEntity1.configurationFor();
        serializer = new DefaultEntitySerializer( configuration );
        objectBuffer = new ObjectBuffer( configuration.getKryo() );
        bo = new BO( (BasicPersistentEntity) configuration.getMappingContext().getPersistentEntity( TestEntity1.class ) );

        return Arrays.asList( new Object[][] {
                { new OffHeapLinearProbingSet( 2, configuration, bo ) },
                { SpaceUtility.parallelizedOffLinearHashSet(
                        configuration,
                        configuration.getMappingContext().getPersistentEntity( TestEntity1.class ),
                        129,
                        16 ) } } );
    }

    @AfterClass
    public static void afterClass()
                                   throws Exception {
        configuration.destroy();
    }

    @SuppressWarnings({})
    public OffHeapHashMapTest(final OffHeapHashSet set) throws Exception {
        heapHashMap = set;
        heapHashMap.afterPropertiesSet();

        if ( !( set instanceof OffHeapLinearProbingSet ) )
            configuration.setExpirationListener( new SpaceExpirationListener() {

                @Override
                public boolean retrieveAsEntity() {
                    return true;
                }

                @Override
                public void handleNotification(final Object entity,
                                               final Class<?> persistentClass,
                                               final long originalTimeToLive) {
                    Assert.assertNotNull( entity );
                    Assert.assertTrue( persistentClass == TestEntity1.class );
                    Assert.assertTrue( originalTimeToLive > 0 );

                    LOGGER.info( "{} has been expired, {}-{}", new Object[] { entity, persistentClass.getSimpleName(), originalTimeToLive } );
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

        bytes1 = serializer.serialize( CacheStoreEntryWrapper.valueOf( bo, configuration, entity1 ), new ObjectBuffer( configuration.getKryo() ) );
        bytes2 = serializer.serialize( CacheStoreEntryWrapper.valueOf( bo, configuration, entity2 ), new ObjectBuffer( configuration.getKryo() ) );
        bytes3 = serializer.serialize( CacheStoreEntryWrapper.valueOf( bo, configuration, entity3 ), new ObjectBuffer( configuration.getKryo() ) );

        p1 = new ByteArrayPointer( bytes1, entity1, Long.MAX_VALUE );
        p2 = new ByteArrayPointer( bytes2, entity2, Long.MAX_VALUE );
        p3 = new ByteArrayPointer( bytes3, entity3, Long.MAX_VALUE );
    }

    @After
    public void after()
                       throws Exception {
        heapHashMap.destroy();
    }

    @Test
    public void canStoreAndRetrieveRemove() {
        heapHashMap.put( key1, p1 );
        heapHashMap.put( key2, p2 );
        heapHashMap.put( key3, p3 );

        ByteArrayPointer object1 = heapHashMap.getAsPointer( key1 );
        ByteArrayPointer object2 = heapHashMap.getAsPointer( key2 );
        ByteArrayPointer object3 = heapHashMap.getAsPointer( key3 );

        assertThat( ByteArrayPointer.getEntityState( object1.dump() ), is( bytes1 ) );
        assertThat( ByteArrayPointer.getEntityState( object2.dump() ), is( bytes2 ) );
        assertThat( ByteArrayPointer.getEntityState( object3.dump() ), is( bytes3 ) );

        Assert.assertTrue( heapHashMap.contains( key1 ) );
        Assert.assertTrue( heapHashMap.contains( key2 ) );
        Assert.assertTrue( heapHashMap.contains( key3 ) );
        Assert.assertFalse( heapHashMap.contains( key1 + key2 + key3 ) );

        heapHashMap.remove( key1 );
        Assert.assertFalse( heapHashMap.contains( key1 ) );

        heapHashMap.remove( UUID.randomUUID().toString() );

        ByteArrayPointer p4 = new ByteArrayPointer( bytes2, entity2, Long.MAX_VALUE );
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

            byte[] bytes = serializer.serialize(
                    CacheStoreEntryWrapper.valueOf( bo, configuration, arr[i] ),
                    new ObjectBuffer( configuration.getKryo() ) );
            ByteArrayPointer p = new ByteArrayPointer( bytes, arr[i], Long.MAX_VALUE );
            heapHashMap.put( arr[i].getUniqueIdentifier(), p );
        }

        for ( int i = 0; i < arr.length / 2; i++ ) {
            Assert.assertTrue( heapHashMap.contains( arr[i].getUniqueIdentifier() ) );
            heapHashMap.remove( arr[i].getUniqueIdentifier() );
            Assert.assertFalse( heapHashMap.contains( arr[i].getUniqueIdentifier() ) );
        }

        heapHashMap.destroy();

        for ( TestEntity1 element : arr ) {
            byte[] bytes = serializer.serialize(
                    CacheStoreEntryWrapper.valueOf( bo, configuration, element ),
                    new ObjectBuffer( configuration.getKryo() ) );
            ByteArrayPointer p = new ByteArrayPointer( bytes, element, Long.MAX_VALUE );

            heapHashMap.put( element.getUniqueIdentifier(), p );
        }
        heapHashMap.destroy();
    }

    @Test
    public void canAddUpTo5000AndRemoveUpTo1OneByOne() {
        TestEntity1[] arr = new TestEntity1[5000];
        for ( int i = 0; i < arr.length; i++ ) {
            arr[i] = new TestEntity1();
            arr[i].afterPropertiesSet();

            byte[] bytes = serializer.serialize(
                    CacheStoreEntryWrapper.valueOf( bo, configuration, arr[i] ),
                    new ObjectBuffer( configuration.getKryo() ) );
            ByteArrayPointer p = new ByteArrayPointer( bytes, arr[i], Long.MAX_VALUE );

            heapHashMap.put( arr[i].getUniqueIdentifier(), p );
        }

        for ( TestEntity1 element : arr )
            heapHashMap.remove( element.getUniqueIdentifier() );
    }

    @Test
    public void canRemoveExpiredEntitiesAutomatically()
                                                       throws InterruptedException {
        TestEntity1[] arr = new TestEntity1[2000];
        for ( int i = 0; i < arr.length; i++ ) {
            arr[i] = new TestEntity1();
            arr[i].afterPropertiesSet();

            byte[] bytes = serializer.serialize(
                    CacheStoreEntryWrapper.valueOf( bo, configuration, arr[i] ),
                    new ObjectBuffer( configuration.getKryo() ) );
            ByteArrayPointer p = new ByteArrayPointer( bytes, arr[i], 1L );

            heapHashMap.put( arr[i].getUniqueIdentifier(), p );
        }
        Thread.sleep( 1 );

        for ( int i = 0; i < arr.length / 2; i++ )
            Assert.assertTrue( heapHashMap.getAsPointer( arr[i].getUniqueIdentifier() ) == null );

        for ( int i = arr.length / 2; i < arr.length; i++ )
            Assert.assertFalse( heapHashMap.contains( arr[i].getUniqueIdentifier() ) );
    }
}
