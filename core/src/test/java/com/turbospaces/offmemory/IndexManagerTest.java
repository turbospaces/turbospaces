package com.turbospaces.offmemory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Collections;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.esotericsoftware.kryo.ObjectBuffer;
import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.api.SpaceMemoryOverflowException;
import com.turbospaces.model.BO;
import com.turbospaces.model.TestEntity1;
import com.turbospaces.offmemory.ByteArrayPointer;
import com.turbospaces.offmemory.IndexManager;
import com.turbospaces.serialization.DefaultEntitySerializer;
import com.turbospaces.spaces.CacheStoreEntryWrapper;
import com.turbospaces.spaces.EntryKeyLockQuard;
import com.turbospaces.spaces.KeyLocker;
import com.turbospaces.spaces.tx.TransactionScopeKeyLocker;

@SuppressWarnings("javadoc")
public class IndexManagerTest {
    IndexManager indexManager;
    DefaultEntitySerializer serializer;
    SpaceConfiguration configuration;
    @SuppressWarnings("rawtypes")
    BO bo;
    KeyLocker keyLocker;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public IndexManagerTest() throws Exception {
        super();

        configuration = TestEntity1.configurationFor();
        indexManager = new IndexManager( configuration.getMappingContext().getPersistentEntity( TestEntity1.class ), configuration );
        serializer = new DefaultEntitySerializer( configuration );
        bo = new BO( (BasicPersistentEntity) configuration.getMappingContext().getPersistentEntity( TestEntity1.class ) );
        keyLocker = new TransactionScopeKeyLocker();
    }

    @After
    public void after()
                       throws Exception {
        indexManager.destroy();
        configuration.destroy();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void canGetSpaceMemoryOverflowException()
                                                    throws Exception {
        configuration = new SpaceConfiguration();
        MongoMappingContext mappingContext = new MongoMappingContext();
        mappingContext.setInitialEntitySet( Collections.singleton( TestEntity1.class ) );
        mappingContext.afterPropertiesSet();
        configuration.setMappingContext( mappingContext );
        configuration.boFor( TestEntity1.class ).getCapacityRestriction().setMaxMemorySizeInMb( 1 );
        configuration.afterPropertiesSet();

        serializer = new DefaultEntitySerializer( configuration );
        indexManager = new IndexManager( configuration.getMappingContext().getPersistentEntity( TestEntity1.class ), configuration );

        ByteArrayPointer pointer = null;
        try {
            while ( System.currentTimeMillis() > 0 ) {
                TestEntity1 entity = new TestEntity1();
                entity.afterPropertiesSet();
                pointer = new ByteArrayPointer( serializer.serialize( CacheStoreEntryWrapper.valueOf( bo, configuration, entity ), new ObjectBuffer(
                        configuration.getKryo() ) ), entity, Long.MAX_VALUE );
                indexManager.add(
                        entity,
                        keyLocker.writeLock( entity.getUniqueIdentifier(), new Random().nextLong(), Integer.MAX_VALUE, true ),
                        pointer );
            }
            Assert.fail();
        }
        catch ( SpaceMemoryOverflowException e ) {
            assertThat( e.getMb(), is( 1 ) );
            assertThat( e.getSerializedState(), is( pointer.getSerializedData() ) );
        }
        finally {
            configuration.destroy();
        }
    }

    @Test
    public void canAddGetAndUpdatePointers() {
        TestEntity1 entity1 = new TestEntity1();
        TestEntity1 entity2 = new TestEntity1();
        TestEntity1 entity3 = new TestEntity1();
        TestEntity1 entity4 = new TestEntity1();

        entity1.afterPropertiesSet();
        entity2.afterPropertiesSet();
        entity3.afterPropertiesSet();
        entity4.afterPropertiesSet();

        entity4.setUniqueIdentifier( entity2.getUniqueIdentifier() );

        ByteArrayPointer pointer1 = new ByteArrayPointer( serializer.serialize(
                CacheStoreEntryWrapper.valueOf( bo, configuration, entity1 ),
                new ObjectBuffer( configuration.getKryo() ) ), entity1, Integer.MAX_VALUE );
        ByteArrayPointer pointer2 = new ByteArrayPointer( serializer.serialize(
                CacheStoreEntryWrapper.valueOf( bo, configuration, entity2 ),
                new ObjectBuffer( configuration.getKryo() ) ), entity2, Integer.MAX_VALUE );
        ByteArrayPointer pointer3 = new ByteArrayPointer( serializer.serialize(
                CacheStoreEntryWrapper.valueOf( bo, configuration, entity3 ),
                new ObjectBuffer( configuration.getKryo() ) ), entity3, Integer.MAX_VALUE );
        ByteArrayPointer pointer4 = new ByteArrayPointer( serializer.serialize(
                CacheStoreEntryWrapper.valueOf( bo, configuration, entity4 ),
                new ObjectBuffer( configuration.getKryo() ) ), entity4, Integer.MAX_VALUE );

        indexManager.add( entity1, keyLocker.writeLock( entity1.getUniqueIdentifier(), 234, Integer.MAX_VALUE, true ), pointer1 );
        indexManager.add( entity2, keyLocker.writeLock( entity2.getUniqueIdentifier(), 235, Integer.MAX_VALUE, true ), pointer2 );
        indexManager.add( entity3, keyLocker.writeLock( entity3.getUniqueIdentifier(), 236, Integer.MAX_VALUE, true ), pointer3 );

        assertThat( indexManager.size(), is( 3L ) );
        assertThat(
                indexManager.offHeapBytesOccuiped(),
                is( (long) ( pointer1.bytesOccupied() + pointer2.bytesOccupied() + pointer3.bytesOccupied() ) ) );

        assertThat( (ByteArrayPointer) indexManager.getByUniqueIdentifier( entity1.getUniqueIdentifier(), true ), is( pointer1 ) );
        assertThat( (ByteArrayPointer) indexManager.getByUniqueIdentifier( entity2.getUniqueIdentifier(), true ), is( pointer2 ) );
        assertThat( (ByteArrayPointer) indexManager.getByUniqueIdentifier( entity3.getUniqueIdentifier(), true ), is( pointer3 ) );

        EntryKeyLockQuard writeLock = keyLocker.writeLock( entity1.getUniqueIdentifier(), 234, Integer.MAX_VALUE, true );
        indexManager.takeByUniqueIdentifier( writeLock );
        assertThat( indexManager.offHeapBytesOccuiped(), is( (long) ( pointer2.bytesOccupied() + pointer3.bytesOccupied() ) ) );
        keyLocker.writeUnlock( writeLock, 234 );

        indexManager.add( entity4, keyLocker.writeLock( entity4.getUniqueIdentifier(), 235, Integer.MAX_VALUE, true ), pointer4 );
        assertThat( indexManager.offHeapBytesOccuiped(), is( (long) ( pointer4.bytesOccupied() + pointer3.bytesOccupied() ) ) );
    }
}
