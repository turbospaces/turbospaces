package com.turbospaces.collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.RoundingMode;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowire;

import com.esotericsoftware.kryo.serialize.EnumSerializer;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import com.turbospaces.api.SpaceExpirationListener;
import com.turbospaces.core.JVMUtil;
import com.turbospaces.model.TestEntity1;
import com.turbospaces.serialization.DecoratedKryo;
import com.turbospaces.serialization.SingleDimensionArraySerializer;

@SuppressWarnings("javadoc")
public class GuavaOffHeapCacheBuilderTest {
    GuavaOffHeapCacheBuilder<String, TestEntity1> builder;
    GuavaOffHeapCache<String, TestEntity1> cache;

    @Before
    public void setup()
                       throws ClassNotFoundException {
        DecoratedKryo kryo = new DecoratedKryo();
        Class<?> cl1 = Class.forName( "[L" + RoundingMode.class.getName() + ";" );
        Class<?> cl2 = Class.forName( "[L" + Autowire.class.getName() + ";" );
        SingleDimensionArraySerializer s1 = new SingleDimensionArraySerializer( cl1, kryo );
        SingleDimensionArraySerializer s2 = new SingleDimensionArraySerializer( cl2, kryo );
        kryo.register( cl1, s1 );
        kryo.register( cl2, s2 );
        kryo.register( Autowire.class, new EnumSerializer( Autowire.class ) );

        builder = new GuavaOffHeapCacheBuilder<String, TestEntity1>();
        builder.expireAfterWrite( Integer.MAX_VALUE, TimeUnit.MILLISECONDS ).kryo( kryo ).recordStats( true );
    }

    @After
    public void destroy() {
        cache.toString();
        cache.cleanUp();
        cache.invalidateAll();
    }

    @Test
    public void trivialSunnyDayScenario()
                                         throws ExecutionException {
        cache = builder.expireAfterWrite( Integer.MAX_VALUE, null ).build( TestEntity1.class );

        for ( int i = 0; i < 100; i++ ) {
            TestEntity1 entity1 = new TestEntity1();
            entity1.afterPropertiesSet();
            cache.put( entity1.getUniqueIdentifier(), entity1 );
            assertThat( cache.size(), is( 1L ) );
            assertThat( cache.getIfPresent( entity1.getUniqueIdentifier() ).getUniqueIdentifier(), is( entity1.getUniqueIdentifier() ) );
            Assert.assertTrue( cache.get( entity1.getUniqueIdentifier(), new Callable<TestEntity1>() {
                @Override
                public TestEntity1 call() {
                    return null;
                }
            } ).getUniqueIdentifier().equals( entity1.getUniqueIdentifier() ) );
            cache.invalidate( entity1.getUniqueIdentifier() );
            Assert.assertTrue( cache.getIfPresent( entity1.getUniqueIdentifier() ) == null );
        }
        assertThat( cache.stats().hitCount(), is( 200L ) );
        assertThat( cache.stats().missCount(), is( 100L ) );
        assertThat( cache.size(), is( 0L ) );
        cache.cleanUp();
        assertThat( cache.size(), is( 0L ) );
    }

    @Test
    public void trivialSunnyDayScenario1()
                                          throws ExecutionException {
        cache = builder.build( TestEntity1.class );
        for ( int i = 0; i < 100; i++ ) {
            final String key = String.valueOf( System.currentTimeMillis() + i );
            assertThat( cache.getIfPresent( key ), is( nullValue() ) );
            cache.get( key, new Callable<TestEntity1>() {
                @Override
                public TestEntity1 call() {
                    Uninterruptibles.sleepUninterruptibly( 10, TimeUnit.NANOSECONDS );
                    TestEntity1 entity = new TestEntity1();
                    entity.afterPropertiesSet();
                    entity.setUniqueIdentifier( key );
                    return entity;
                }
            } );
            assertThat( (int) cache.size(), is( i + 1 ) );
            assertThat( cache.getIfPresent( key ).getUniqueIdentifier(), is( key ) );
        }
        assertThat( cache.stats().hitCount(), is( 100L ) );
        assertThat( cache.stats().loadCount(), is( 100L ) );
        assertThat( cache.stats().missCount(), is( 200L ) );
        assertThat( cache.stats().averageLoadPenalty(), is( greaterThan( 0D ) ) );
    }

    @Test
    public void trivialSunnyDayScenario2() {
        cache = builder.executorService( MoreExecutors.sameThreadExecutor() ).build( TestEntity1.class );
        final TestEntity1[] entities = new TestEntity1[2048];
        Assert.assertTrue( JVMUtil.repeatConcurrently( Runtime.getRuntime().availableProcessors(), entities.length, new Function<Integer, Object>() {
            @Override
            public Object apply(final Integer input) {
                TestEntity1 entity1 = new TestEntity1();
                entity1.afterPropertiesSet();
                cache.put( entity1.getUniqueIdentifier(), entity1 );
                entities[input] = entity1;

                assertThat( cache.getIfPresent( entity1.getUniqueIdentifier() ).getUniqueIdentifier(), is( entity1.getUniqueIdentifier() ) );
                try {
                    Assert.assertTrue( cache.get( entity1.getUniqueIdentifier(), new Callable<TestEntity1>() {
                        @Override
                        public TestEntity1 call() {
                            return null;
                        }
                    } ).getUniqueIdentifier().equals( entity1.getUniqueIdentifier() ) );
                }
                catch ( ExecutionException e ) {
                    Throwables.propagate( e );
                }
                return null;
            }
        } ).isEmpty() );
        Assert.assertTrue( JVMUtil.repeatConcurrently( Runtime.getRuntime().availableProcessors(), entities.length, new Function<Integer, Object>() {
            @Override
            public Object apply(final Integer input) {
                cache.invalidate( entities[input].getUniqueIdentifier() );
                assertThat( cache.getIfPresent( entities[input].getUniqueIdentifier() ), is( nullValue() ) );
                return null;
            }
        } ).isEmpty() );

        assertThat( cache.size(), is( 0L ) );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void handlesLoadingExceptionProperly()
                                                 throws Exception {
        cache = builder.build( TestEntity1.class );
        Callable<TestEntity1> loader = mock( Callable.class );
        when( loader.call() ).thenThrow( new IllegalStateException( "failed to load 123 key" ) );
        try {
            cache.get( "123", loader );
            Assert.fail();
        }
        catch ( ExecutionException e ) {
            assertThat( cache.stats().loadExceptionCount(), is( 1L ) );
            assertThat( e.getCause().getClass().getName(), is( IllegalStateException.class.getName() ) );
        }
    }

    @Test
    public void testExpiration()
                                throws InterruptedException {
        ExecutorService threadPool = Executors.newCachedThreadPool();
        builder
                .executorService( threadPool )
                .expireAfterWrite( 1, TimeUnit.MILLISECONDS )
                .expirationListener( new SpaceExpirationListener<String, TestEntity1>() {
                    @Override
                    public void handleNotification(final TestEntity1 entity,
                                                   final String id,
                                                   final Class<TestEntity1> persistentClass,
                                                   final int originalTimeToLive) {
                        Assert.assertTrue( entity.getClass() == TestEntity1.class );
                        assertThat( entity.getUniqueIdentifier(), is( id ) );
                    }
                } );
        cache = builder.build( TestEntity1.class );

        final TestEntity1[] entities = new TestEntity1[128];
        for ( int i = 0; i < entities.length; i++ ) {
            TestEntity1 entity1 = new TestEntity1();
            entity1.afterPropertiesSet();
            entity1.setUniqueIdentifier( entity1.getUniqueIdentifier() + i );
            entities[i] = entity1;
            cache.put( entity1.getUniqueIdentifier(), entity1 );
        }

        Thread.sleep( 1 );
        cache.cleanUp();

        threadPool.shutdown();
        threadPool.awaitTermination( 10, TimeUnit.SECONDS );
        assertThat( cache.size(), is( 0L ) );
        assertThat( cache.stats().evictionCount(), is( (long) entities.length ) );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void canLoadKeysValueConcurrently()
                                              throws Exception {
        cache = builder.recordStats( false ).build( TestEntity1.class );
        final TestEntity1 entity1 = new TestEntity1();
        entity1.afterPropertiesSet();
        final Callable<TestEntity1> loader = mock( Callable.class );
        when( loader.call() ).thenReturn( entity1 );
        Assert.assertTrue( JVMUtil.repeatConcurrently( 128, 256, new Runnable() {
            @Override
            public void run() {
                try {
                    cache.get( entity1.getUniqueIdentifier(), loader );
                }
                catch ( ExecutionException e ) {
                    Throwables.propagate( e );
                }
            }
        } ).isEmpty() );
        verify( loader ).call();
    }
}
