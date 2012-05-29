package com.turbospaces.offmemory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;

import com.turbospaces.api.CapacityRestriction;
import com.turbospaces.api.EmbeddedJSpaceRunnerTest;
import com.turbospaces.api.JSpace;
import com.turbospaces.api.SpaceCapacityOverflowException;
import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.core.JVMUtil;
import com.turbospaces.model.BO;
import com.turbospaces.model.CacheStoreEntryWrapper;
import com.turbospaces.model.TestEntity1;
import com.turbospaces.spaces.CountingSpaceNotificationListener;
import com.turbospaces.spaces.NotificationContext;
import com.turbospaces.spaces.SpaceCapacityRestrictionHolder;
import com.turbospaces.spaces.tx.TransactionModificationContext;

@RunWith(Parameterized.class)
@SuppressWarnings({ "javadoc", "rawtypes" })
public class DirectOffHeapByteBufferTest {
    OffHeapCacheStore buffer;
    SpaceConfiguration configuration;
    boolean matchById;
    BO bo;

    public DirectOffHeapByteBufferTest(final boolean matchById) {
        this.matchById = matchById;
    }

    @Parameters
    public static List<Object[]> data() {
        return Arrays.asList( new Object[][] { { Boolean.TRUE } } );
    }

    @SuppressWarnings("unchecked")
    @Before
    public void before()
                        throws Exception {
        configuration = EmbeddedJSpaceRunnerTest.configurationFor();
        buffer = new OffHeapCacheStore( configuration, TestEntity1.class, new SpaceCapacityRestrictionHolder( configuration.getCapacityRestriction() ) );
        bo = new BO( (BasicPersistentEntity) configuration.getMappingContext().getPersistentEntity( TestEntity1.class ) );
        buffer.afterPropertiesSet();
    }

    @After
    public void after()
                       throws Exception {
        buffer.toString();
        assertThat( buffer.getIndexManager(), is( notNullValue() ) );
        buffer.destroy();
        configuration.destroy();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void canGetSpaceOverflowException()
                                              throws Exception {
        TransactionModificationContext modificationContext = new TransactionModificationContext();
        buffer.destroy();
        configuration = new SpaceConfiguration();
        MongoMappingContext mappingContext = new MongoMappingContext();
        mappingContext.setInitialEntitySet( Collections.singleton( TestEntity1.class ) );
        mappingContext.afterPropertiesSet();
        configuration.setMappingContext( mappingContext );
        CapacityRestriction capacityRestriction = new CapacityRestriction();
        capacityRestriction.setMaxElements( 1 );
        configuration.restrictCapacity( TestEntity1.class, capacityRestriction );
        configuration.afterPropertiesSet();
        buffer = new OffHeapCacheStore( configuration, TestEntity1.class, new SpaceCapacityRestrictionHolder( configuration.getCapacityRestriction() ) );
        buffer.afterPropertiesSet();

        TestEntity1 entity1 = new TestEntity1();
        TestEntity1 entity2 = new TestEntity1();

        entity1.afterPropertiesSet();
        entity2.afterPropertiesSet();

        buffer.write(
                CacheStoreEntryWrapper.writeValueOf( bo, entity1 ),
                modificationContext,
                JSpace.LEASE_FOREVER,
                Integer.MAX_VALUE,
                JSpace.WRITE_OR_UPDATE );
        buffer.write(
                CacheStoreEntryWrapper.writeValueOf( bo, entity2 ),
                modificationContext,
                JSpace.LEASE_FOREVER,
                Integer.MAX_VALUE,
                JSpace.WRITE_OR_UPDATE );

        try {
            modificationContext.flush( buffer );
            Assert.fail();
        }
        catch ( SpaceCapacityOverflowException e ) {
            assertThat( e.getMaxElements(), is( configuration.boFor( TestEntity1.class ).getCapacityRestriction().getMaxElements() ) );
            assertThat( e.getObj(), anyOf( is( (Object) entity2 ), is( (Object) entity1 ) ) );
        }
        finally {
            configuration.destroy();
        }
    }

    @Test
    public void bahaveCorrectly()
                                 throws InterruptedException {
        TransactionModificationContext modificationContext = new TransactionModificationContext();
        TestEntity1 entity = new TestEntity1();
        entity.afterPropertiesSet();

        int takeModifier = JSpace.TAKE_ONLY;
        int readModifier = JSpace.READ_ONLY;
        if ( matchById ) {
            takeModifier = takeModifier | JSpace.MATCH_BY_ID;
            readModifier = readModifier | JSpace.MATCH_BY_ID;
        }

        CacheStoreEntryWrapper wrapper = CacheStoreEntryWrapper.writeValueOf( bo, entity );
        buffer.write( wrapper, modificationContext, JSpace.LEASE_FOREVER, Integer.MAX_VALUE, JSpace.WRITE_ONLY );
        buffer.write( wrapper, modificationContext, JSpace.LEASE_FOREVER, Integer.MAX_VALUE, JSpace.UPDATE_ONLY );
        buffer.fetch( wrapper, modificationContext, 0, 1, takeModifier );
        buffer.write( wrapper, modificationContext, JSpace.LEASE_FOREVER, Integer.MAX_VALUE, JSpace.WRITE_ONLY );
        buffer.write( wrapper, modificationContext, JSpace.LEASE_FOREVER, Integer.MAX_VALUE, JSpace.UPDATE_ONLY );
        buffer.fetch( wrapper, modificationContext, 0, 1, takeModifier );

        Assert.assertTrue( modificationContext.isDirty() );
        modificationContext.flush( buffer );

        entity.s1 = UUID.randomUUID().toString();
        buffer.write( wrapper, modificationContext, JSpace.LEASE_FOREVER, Integer.MAX_VALUE, JSpace.WRITE_ONLY );
        CountingSpaceNotificationListener listener = new CountingSpaceNotificationListener();
        modificationContext.flush(
                buffer,
                Collections.singleton( new NotificationContext( CacheStoreEntryWrapper.writeValueOf( bo, entity ), listener, readModifier ) ) );
        Thread.sleep( 10 );
        assertThat( listener.getWrites().size(), is( 1 ) );

        buffer.write( wrapper, modificationContext, JSpace.LEASE_FOREVER, Integer.MAX_VALUE, JSpace.UPDATE_ONLY );
        listener = new CountingSpaceNotificationListener();
        modificationContext.flush(
                buffer,
                Collections.singleton( new NotificationContext( CacheStoreEntryWrapper.writeValueOf( bo, entity ), listener, readModifier ) ) );
        Thread.sleep( 10 );
        assertThat( listener.getChanges().size(), is( 1 ) );

        buffer.write( wrapper, modificationContext, JSpace.LEASE_FOREVER, Integer.MAX_VALUE, JSpace.UPDATE_ONLY );
        buffer.fetch( wrapper, modificationContext, 0, 1, takeModifier );
        assertThat( buffer.fetch( wrapper, modificationContext, 0, 1, readModifier ), is( nullValue() ) );
        Assert.assertTrue( modificationContext.isDirty() );

        listener = new CountingSpaceNotificationListener();
        modificationContext.flush(
                buffer,
                Collections.singleton( new NotificationContext( CacheStoreEntryWrapper.writeValueOf( bo, entity ), listener, readModifier ) ) );
        Thread.sleep( 10 );
        assertThat( listener.getTakes().size(), is( 1 ) );
    }

    @Test
    public void canAdd3EntitiesAndFindByItself()
                                                throws CloneNotSupportedException,
                                                InterruptedException {
        TransactionModificationContext modificationContext = new TransactionModificationContext();
        TestEntity1 entity1 = new TestEntity1();
        TestEntity1 entity2 = new TestEntity1();
        TestEntity1 entity3 = new TestEntity1();

        entity1.afterPropertiesSet();
        entity2.afterPropertiesSet();
        entity3.afterPropertiesSet();

        buffer.write(
                CacheStoreEntryWrapper.writeValueOf( bo, entity1 ),
                modificationContext,
                JSpace.LEASE_FOREVER,
                Integer.MAX_VALUE,
                JSpace.WRITE_OR_UPDATE );
        buffer.write(
                CacheStoreEntryWrapper.writeValueOf( bo, entity2 ),
                modificationContext,
                JSpace.LEASE_FOREVER,
                Integer.MAX_VALUE,
                JSpace.WRITE_OR_UPDATE );
        buffer.write(
                CacheStoreEntryWrapper.writeValueOf( bo, entity3 ),
                modificationContext,
                JSpace.LEASE_FOREVER,
                Integer.MAX_VALUE,
                JSpace.WRITE_OR_UPDATE );

        TestEntity1 entity1Clone = entity1.clone();
        TestEntity1 entity2Clone = entity2.clone();
        TestEntity1 entity3Clone = entity3.clone();

        int modifier = JSpace.READ_ONLY;
        if ( matchById )
            modifier = modifier | JSpace.MATCH_BY_ID;

        ByteBuffer[] match1 = buffer.fetch( CacheStoreEntryWrapper.writeValueOf( bo, entity1Clone ), modificationContext, 0, 1, modifier );
        ByteBuffer[] match2 = buffer.fetch( CacheStoreEntryWrapper.writeValueOf( bo, entity2Clone ), modificationContext, 0, 1, modifier );
        ByteBuffer[] match3 = buffer.fetch( CacheStoreEntryWrapper.writeValueOf( bo, entity3Clone ), modificationContext, 0, 1, modifier );

        assertThat( match1.length, is( 1 ) );
        assertThat( match2.length, is( 1 ) );
        assertThat( match3.length, is( 1 ) );

        ( (TestEntity1) configuration.getKryo().deserialize( match1[0], TestEntity1.class ).getObject() ).assertMatch( entity1 );
        ( (TestEntity1) configuration.getKryo().deserialize( match2[0], TestEntity1.class ).getObject() ).assertMatch( entity2 );
        ( (TestEntity1) configuration.getKryo().deserialize( match3[0], TestEntity1.class ).getObject() ).assertMatch( entity3 );

        buffer.stats();
        CountingSpaceNotificationListener listener = new CountingSpaceNotificationListener();
        modificationContext.flush(
                buffer,
                Collections.singleton( new NotificationContext( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ), listener, modifier ) ) );
        Thread.sleep( 10 );
        assertThat( listener.getWrites().size(), is( 1 ) );
    }

    @Test(expected = DuplicateKeyException.class)
    public void canGetDuplicateExceptionInContextOfSingleTransaction() {
        TransactionModificationContext modificationContext = new TransactionModificationContext();

        TestEntity1 entity1 = new TestEntity1();
        TestEntity1 entity2 = new TestEntity1();

        entity1.afterPropertiesSet();
        entity2.afterPropertiesSet();
        entity2.setUniqueIdentifier( entity1.getUniqueIdentifier() );

        buffer.write(
                CacheStoreEntryWrapper.writeValueOf( bo, entity1 ),
                modificationContext,
                JSpace.LEASE_FOREVER,
                Integer.MAX_VALUE,
                JSpace.WRITE_ONLY );
        buffer.write(
                CacheStoreEntryWrapper.writeValueOf( bo, entity2 ),
                modificationContext,
                JSpace.LEASE_FOREVER,
                Integer.MAX_VALUE,
                JSpace.WRITE_ONLY );
    }

    @Test(expected = CannotAcquireLockException.class)
    public void canGetAcquireWriteLockExceptionInContextOfParallelTransactionsForWrite()
                                                                                        throws Exception {
        TransactionModificationContext modificationContext1 = new TransactionModificationContext();
        TestEntity1 entity1 = new TestEntity1();

        final TestEntity1 entity2 = new TestEntity1();
        final TransactionModificationContext modificationContext2 = new TransactionModificationContext();

        entity1.afterPropertiesSet();
        entity2.afterPropertiesSet();
        entity2.setUniqueIdentifier( entity1.getUniqueIdentifier() );

        buffer.write(
                CacheStoreEntryWrapper.writeValueOf( bo, entity1 ),
                modificationContext1,
                JSpace.LEASE_FOREVER,
                Integer.MAX_VALUE,
                JSpace.WRITE_ONLY );
        throw JVMUtil.runAndGetExecutionException( new Runnable() {
            @Override
            public void run() {
                try {
                    buffer.write(
                            CacheStoreEntryWrapper.writeValueOf( bo, entity2 ),
                            modificationContext2,
                            JSpace.LEASE_FOREVER,
                            1,
                            JSpace.WRITE_ONLY );
                }
                finally {
                    modificationContext2.discard( buffer );
                }
            }
        } );
    }

    @Test(expected = DuplicateKeyException.class)
    public void canGetDuplicateExceptionWithWriteOnlyModifier() {
        TransactionModificationContext modificationContext = new TransactionModificationContext();
        TestEntity1 entity1 = new TestEntity1();
        TestEntity1 entity2 = new TestEntity1();
        entity1.afterPropertiesSet();
        entity2.afterPropertiesSet();
        entity2.setUniqueIdentifier( entity1.getUniqueIdentifier() );

        buffer.write(
                CacheStoreEntryWrapper.writeValueOf( bo, entity1 ),
                modificationContext,
                JSpace.LEASE_FOREVER,
                Integer.MAX_VALUE,
                JSpace.WRITE_ONLY );
        modificationContext.flush( buffer );
        try {
            buffer.write(
                    CacheStoreEntryWrapper.writeValueOf( bo, entity2 ),
                    modificationContext,
                    JSpace.LEASE_FOREVER,
                    Integer.MAX_VALUE,
                    JSpace.WRITE_ONLY );
        }
        finally {
            modificationContext.discard( buffer );
        }
    }

    @Test(expected = DataRetrievalFailureException.class)
    public void canGetObjectRetrieveExceptionWithUpdateOnlyModifier() {
        TransactionModificationContext modificationContext = new TransactionModificationContext();
        TestEntity1 entity = new TestEntity1();
        entity.afterPropertiesSet();

        try {
            buffer.write(
                    CacheStoreEntryWrapper.writeValueOf( bo, entity ),
                    modificationContext,
                    JSpace.LEASE_FOREVER,
                    Integer.MAX_VALUE,
                    JSpace.UPDATE_ONLY );
        }
        finally {
            modificationContext.discard( buffer );
        }
    }

    @Test
    public void puttingObjectByTheSameUniqueIdentifierBehavesAsOverride()
                                                                         throws CloneNotSupportedException {
        TransactionModificationContext modificationContext = new TransactionModificationContext();
        TestEntity1 entity = new TestEntity1();
        entity.afterPropertiesSet();

        buffer.write(
                CacheStoreEntryWrapper.writeValueOf( bo, entity ),
                modificationContext,
                JSpace.LEASE_FOREVER,
                Integer.MAX_VALUE,
                JSpace.WRITE_ONLY );
        buffer.write(
                CacheStoreEntryWrapper.writeValueOf( bo, entity.clone() ),
                modificationContext,
                JSpace.LEASE_FOREVER,
                Integer.MAX_VALUE,
                JSpace.WRITE_OR_UPDATE );
        modificationContext.flush( buffer );

        buffer.write(
                CacheStoreEntryWrapper.writeValueOf( bo, entity.clone() ),
                modificationContext,
                JSpace.LEASE_FOREVER,
                Integer.MAX_VALUE,
                JSpace.UPDATE_ONLY );
        buffer.write(
                CacheStoreEntryWrapper.writeValueOf( bo, entity.clone() ),
                modificationContext,
                JSpace.LEASE_FOREVER,
                Integer.MAX_VALUE,
                JSpace.WRITE_OR_UPDATE );
        buffer.write(
                CacheStoreEntryWrapper.writeValueOf( bo, entity.clone() ),
                modificationContext,
                JSpace.LEASE_FOREVER,
                Integer.MAX_VALUE,
                JSpace.UPDATE_ONLY );
        modificationContext.flush( buffer );
    }

    @Test
    public void canAddEntitiesAndTakeByItself() {
        TransactionModificationContext modificationContext = new TransactionModificationContext();
        TestEntity1 entity1 = new TestEntity1();
        TestEntity1 entity2 = new TestEntity1();
        TestEntity1 entity3 = new TestEntity1();

        entity1.afterPropertiesSet();
        entity2.afterPropertiesSet();
        entity3.afterPropertiesSet();

        buffer.write(
                CacheStoreEntryWrapper.writeValueOf( bo, entity1 ),
                modificationContext,
                JSpace.LEASE_FOREVER,
                Integer.MAX_VALUE,
                JSpace.WRITE_OR_UPDATE );
        buffer.write(
                CacheStoreEntryWrapper.writeValueOf( bo, entity2 ),
                modificationContext,
                JSpace.LEASE_FOREVER,
                Integer.MAX_VALUE,
                JSpace.WRITE_OR_UPDATE );
        buffer.write(
                CacheStoreEntryWrapper.writeValueOf( bo, entity3 ),
                modificationContext,
                JSpace.LEASE_FOREVER,
                Integer.MAX_VALUE,
                JSpace.WRITE_OR_UPDATE );

        int modifier = JSpace.EXCLUSIVE_READ_LOCK;
        if ( matchById )
            modifier = modifier | JSpace.MATCH_BY_ID;

        ByteBuffer[] match1 = buffer.fetch( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ), modificationContext, 0, 1, modifier );
        ByteBuffer[] match2 = buffer.fetch( CacheStoreEntryWrapper.writeValueOf( bo, entity2 ), modificationContext, 0, 1, modifier );
        ByteBuffer[] match3 = buffer.fetch( CacheStoreEntryWrapper.writeValueOf( bo, entity3 ), modificationContext, 0, 1, modifier );

        ( (TestEntity1) configuration.getKryo().deserialize( match1[0], TestEntity1.class ).getObject() ).assertMatch( entity1 );
        ( (TestEntity1) configuration.getKryo().deserialize( match2[0], TestEntity1.class ).getObject() ).assertMatch( entity2 );
        ( (TestEntity1) configuration.getKryo().deserialize( match3[0], TestEntity1.class ).getObject() ).assertMatch( entity3 );

        modifier = JSpace.TAKE_ONLY;
        if ( matchById )
            modifier = modifier | JSpace.MATCH_BY_ID;

        match1 = buffer.fetch( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ), modificationContext, 0, 1, modifier );
        match2 = buffer.fetch( CacheStoreEntryWrapper.writeValueOf( bo, entity2 ), modificationContext, 0, 1, modifier );
        match3 = buffer.fetch( CacheStoreEntryWrapper.writeValueOf( bo, entity3 ), modificationContext, 0, 1, modifier );

        ( (TestEntity1) configuration.getKryo().deserialize( match1[0], TestEntity1.class ).getObject() ).assertMatch( entity1 );
        ( (TestEntity1) configuration.getKryo().deserialize( match2[0], TestEntity1.class ).getObject() ).assertMatch( entity2 );
        ( (TestEntity1) configuration.getKryo().deserialize( match3[0], TestEntity1.class ).getObject() ).assertMatch( entity3 );

        modificationContext.flush( buffer );

        assertThat( match1.length, is( 1 ) );
        assertThat( match2.length, is( 1 ) );
        assertThat( match3.length, is( 1 ) );
    }

    @Test(expected = CannotAcquireLockException.class)
    public void canGetAcquireWriteLockExceptionInContextOfParallelTransactionsForTake()
                                                                                       throws Exception {
        final TransactionModificationContext modificationContext1 = new TransactionModificationContext();
        final TransactionModificationContext modificationContext2 = new TransactionModificationContext();

        final TestEntity1 entity1 = new TestEntity1();
        entity1.afterPropertiesSet();

        buffer.write(
                CacheStoreEntryWrapper.writeValueOf( bo, entity1 ),
                modificationContext1,
                JSpace.LEASE_FOREVER,
                Integer.MAX_VALUE,
                JSpace.WRITE_ONLY );
        modificationContext1.flush( buffer );

        final AtomicInteger modifier = new AtomicInteger( JSpace.TAKE_ONLY );
        if ( matchById )
            modifier.set( modifier.intValue() | JSpace.MATCH_BY_ID );
        // lock entity for delete, but do not flush transaction modification context immediately
        buffer.fetch( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ), modificationContext1, Integer.MAX_VALUE, 0, modifier.intValue() );
        throw JVMUtil.runAndGetExecutionException( new Runnable() {

            @Override
            public void run() {
                try {
                    int timeout = 1;
                    buffer.fetch( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ), modificationContext2, timeout, 10, modifier.intValue() );
                }
                finally {
                    modificationContext2.discard( buffer );
                }
            }
        } );
    }

    @Test(expected = CannotAcquireLockException.class)
    public void canGetAcquireWriteLockExceptionInContextOfParallelTransactionsForExclusiveRead()
                                                                                                throws Exception {
        final TransactionModificationContext modificationContext1 = new TransactionModificationContext();
        final TransactionModificationContext modificationContext2 = new TransactionModificationContext();

        final TestEntity1 entity1 = new TestEntity1();
        entity1.afterPropertiesSet();

        buffer.write(
                CacheStoreEntryWrapper.writeValueOf( bo, entity1 ),
                modificationContext1,
                JSpace.LEASE_FOREVER,
                Integer.MAX_VALUE,
                JSpace.WRITE_ONLY );
        modificationContext1.flush( buffer );

        final AtomicInteger modifier = new AtomicInteger( JSpace.EXCLUSIVE_READ_LOCK );
        if ( matchById )
            modifier.set( modifier.intValue() | JSpace.MATCH_BY_ID );
        buffer.fetch( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ), modificationContext1, Integer.MAX_VALUE, 1, modifier.intValue() );
        try {
            throw JVMUtil.runAndGetExecutionException( new Runnable() {

                @Override
                public void run() {
                    try {
                        int timeout = 1;
                        buffer.fetch( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ), modificationContext2, timeout, 1, modifier.intValue() );
                    }
                    finally {
                        modificationContext2.discard( buffer );
                    }
                }
            } );
        }
        finally {
            System.out.println( modificationContext1 );
            modificationContext2.flush( buffer );
            modificationContext1.flush( buffer );
        }
    }

    @Test
    public void canReleaseLockForPreviouslyUnstorredId() {
        final TransactionModificationContext modificationContext = new TransactionModificationContext();
        final TestEntity1 entity = new TestEntity1();
        entity.afterPropertiesSet();

        int takeModifier = JSpace.TAKE_ONLY;
        int readModifier = JSpace.EXCLUSIVE_READ_LOCK;
        if ( matchById ) {
            takeModifier = takeModifier | JSpace.MATCH_BY_ID;
            readModifier = readModifier | JSpace.MATCH_BY_ID;
        }

        buffer.fetch( CacheStoreEntryWrapper.writeValueOf( bo, entity ), modificationContext, Integer.MAX_VALUE, 1, takeModifier );
        buffer.fetch( CacheStoreEntryWrapper.writeValueOf( bo, entity ), modificationContext, Integer.MAX_VALUE, 1, readModifier );
    }

    @Test
    public void evictionWorksForReadTakeEvictOperations()
                                                         throws InterruptedException {
        final TransactionModificationContext modificationContext = new TransactionModificationContext();
        final TestEntity1 entity1 = new TestEntity1();
        final TestEntity1 entity2 = new TestEntity1();
        entity1.afterPropertiesSet();
        entity2.afterPropertiesSet();

        buffer.write( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ), modificationContext, 1, 234234, JSpace.WRITE_OR_UPDATE );
        buffer.write( CacheStoreEntryWrapper.writeValueOf( bo, entity2 ), modificationContext, 1, 234234, JSpace.WRITE_OR_UPDATE );
        modificationContext.flush( buffer );

        Thread.sleep( 2 );

        int takeModifier = JSpace.TAKE_ONLY;
        int readModifier = JSpace.EXCLUSIVE_READ_LOCK;
        if ( matchById ) {
            takeModifier = takeModifier | JSpace.MATCH_BY_ID;
            readModifier = readModifier | JSpace.MATCH_BY_ID;
        }

        ByteBuffer[] list = buffer
                .fetch( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ), modificationContext, Integer.MAX_VALUE, 1, takeModifier );
        assertThat( list, is( nullValue() ) );
        list = buffer.fetch( CacheStoreEntryWrapper.writeValueOf( bo, entity2 ), modificationContext, Integer.MAX_VALUE, 1, readModifier );
        assertThat( list, is( nullValue() ) );
    }
}
