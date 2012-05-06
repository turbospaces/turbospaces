/**
 * Copyright (C) 2011 Andrey Borisov <aandrey.borisov@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.turbospaces.serialization;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.UUID;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mapping.model.BasicPersistentEntity;

import com.esotericsoftware.kryo.ObjectBuffer;
import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.core.SpaceUtility;
import com.turbospaces.model.BO;
import com.turbospaces.model.TestEntity1;
import com.turbospaces.serialization.PropertiesSerializer;
import com.turbospaces.spaces.CacheStoreEntryWrapper;

@SuppressWarnings("javadoc")
public class PropertiesSerializerTest {
    Logger logger = LoggerFactory.getLogger( getClass() );
    TestEntity1 entity1;
    @SuppressWarnings("rawtypes")
    BO bo;
    PropertiesSerializer serializer;
    SpaceConfiguration configuration;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Before
    public void before()
                        throws Exception {
        configuration = TestEntity1.configurationFor();
        serializer = new PropertiesSerializer( configuration, new BO( (BasicPersistentEntity) configuration.getMappingContext().getPersistentEntity(
                TestEntity1.class ) ) );
        bo = new BO( (BasicPersistentEntity) configuration.getMappingContext().getPersistentEntity( TestEntity1.class ) );

        entity1 = new TestEntity1();
        entity1.afterPropertiesSet();

        SpaceUtility.gc();
    }

    @After
    public void after()
                       throws Exception {
        configuration.destroy();
    }

    @Test
    public void canSerializeEntity() {
        ObjectBuffer objectBuffer = new ObjectBuffer( configuration.getKryo() );
        byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.valueOf( bo, configuration, entity1 ) );

        logger.info( "wrote {}.({} bytes) to {}", new Object[] { entity1, data.length, Arrays.toString( data ) } );
    }

    @Test
    public void canSerializeDirecyEntity() {
        TestEntity1 entity1 = new TestEntity1();
        entity1.afterPropertiesSet();
        ObjectBuffer objectBuffer = new ObjectBuffer( configuration.getKryo() );
        byte[] data = objectBuffer.writeObjectData( entity1 );
        TestEntity1 clone = objectBuffer.readObjectData( data, entity1.getClass() );
        clone.assertMatch( entity1 );
    }

    @Test
    public void canReadIdProperty() {
        ObjectBuffer objectBuffer = new ObjectBuffer( configuration.getKryo() );
        byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.valueOf( bo, configuration, entity1 ) );

        assertThat( serializer.readID( ByteBuffer.wrap( data ) ), is( notNullValue() ) );
    }

    @Test
    public void canDeSerializeEntity() {
        ObjectBuffer objectBuffer = new ObjectBuffer( configuration.getKryo() );
        byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.valueOf( bo, configuration, entity1 ) );

        TestEntity1 data2 = objectBuffer.readObjectData( data, TestEntity1.class );
        entity1.assertMatch( data2 );
    }

    @Test
    public void canMatchByTempateIfNoFieldProvidedExceptPrimitives() {
        ObjectBuffer objectBuffer = new ObjectBuffer( configuration.getKryo() );
        byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.valueOf( bo, configuration, entity1 ) );

        TestEntity1 template = new TestEntity1();
        template.cleanBeanProperties();

        template.fi1 = entity1.fi1;
        template.fi2 = entity1.fi2;

        CacheStoreEntryWrapper cacheStoreEntryWrapper = CacheStoreEntryWrapper.valueOf( bo, configuration, template );
        Assert.assertTrue( serializer.match( ByteBuffer.wrap( data ), cacheStoreEntryWrapper, false ) != null );
    }

    @Test
    public void cantMatchByTemplateIfPrimitivesDidntMatch() {
        ObjectBuffer objectBuffer = new ObjectBuffer( configuration.getKryo() );
        byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.valueOf( bo, configuration, entity1 ) );

        TestEntity1 template = new TestEntity1();
        template.cleanBeanProperties();

        CacheStoreEntryWrapper cacheStoreEntryWrapper = CacheStoreEntryWrapper.valueOf( bo, configuration, template );
        Assert.assertFalse( serializer.match( ByteBuffer.wrap( data ), cacheStoreEntryWrapper, false ) != null );
    }

    @Test
    public void canMatchByTempateIfPrimaryKeyProvided() {
        ObjectBuffer objectBuffer = new ObjectBuffer( configuration.getKryo() );
        byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.valueOf( bo, configuration, entity1 ) );

        TestEntity1 template = new TestEntity1();
        template.cleanBeanProperties();

        template.fi1 = entity1.fi1;
        template.fi2 = entity1.fi2;
        template.uniqueIdentifier = entity1.getUniqueIdentifier();

        CacheStoreEntryWrapper cacheStoreEntryWrapper = CacheStoreEntryWrapper.valueOf( bo, configuration, template );
        Assert.assertTrue( serializer.match( ByteBuffer.wrap( data ), cacheStoreEntryWrapper, false ) != null );
    }

    @Test
    public void canMatchByTempateIfPrimaryKeyAndVersionProvided() {
        entity1.optimisticLockVersion = 123;
        ObjectBuffer objectBuffer = new ObjectBuffer( configuration.getKryo() );
        byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.valueOf( bo, configuration, entity1 ) );

        TestEntity1 template = new TestEntity1();
        template.cleanBeanProperties();

        template.fi1 = entity1.fi1;
        template.fi2 = entity1.fi2;
        template.uniqueIdentifier = entity1.getUniqueIdentifier();
        template.optimisticLockVersion = entity1.getOptimisticLockVersion();

        CacheStoreEntryWrapper cacheStoreEntryWrapper = CacheStoreEntryWrapper.valueOf( bo, configuration, template );
        Assert.assertTrue( serializer.match( ByteBuffer.wrap( data ), cacheStoreEntryWrapper, false ) != null );
    }

    @Test
    public void canMatchByTempateIfStringProvided() {
        ObjectBuffer objectBuffer = new ObjectBuffer( configuration.getKryo() );
        byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.valueOf( bo, configuration, entity1 ) );

        TestEntity1 template = new TestEntity1();
        template.cleanBeanProperties();

        template.fi1 = entity1.fi1;
        template.fi2 = entity1.fi2;
        template.s1 = entity1.s1;
        template.s4 = entity1.s4;

        Assert.assertTrue( serializer.match( ByteBuffer.wrap( data ), CacheStoreEntryWrapper.valueOf( bo, configuration, template ), false ) != null );
        template.s2 = UUID.randomUUID().toString();
        Assert
                .assertFalse( serializer.match( ByteBuffer.wrap( data ), CacheStoreEntryWrapper.valueOf( bo, configuration, template ), false ) != null );
    }

    @Test
    public void canMatchByTempateIfLongsProvided() {
        ObjectBuffer objectBuffer = new ObjectBuffer( configuration.getKryo() );
        byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.valueOf( bo, configuration, entity1 ) );

        TestEntity1 template = new TestEntity1();
        template.cleanBeanProperties();

        template.fi1 = entity1.fi1;
        template.fi2 = entity1.fi2;
        template.s1 = entity1.s1;
        template.s4 = entity1.s4;
        template.l1 = entity1.l1;
        template.l4 = entity1.l4;

        Assert.assertTrue( serializer.match( ByteBuffer.wrap( data ), CacheStoreEntryWrapper.valueOf( bo, configuration, template ), false ) != null );

        template.l2 = System.currentTimeMillis();
        Assert
                .assertFalse( serializer.match( ByteBuffer.wrap( data ), CacheStoreEntryWrapper.valueOf( bo, configuration, template ), false ) != null );
    }

    @Test
    public void canMatchByTempateIfDatesProvided() {
        ObjectBuffer objectBuffer = new ObjectBuffer( configuration.getKryo() );
        byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.valueOf( bo, configuration, entity1 ) );

        TestEntity1 template = new TestEntity1();
        template.cleanBeanProperties();

        template.fi1 = entity1.fi1;
        template.fi2 = entity1.fi2;
        template.s1 = entity1.s1;
        template.s4 = entity1.s4;
        template.l1 = entity1.l1;
        template.l4 = entity1.l4;
        template.dt1 = entity1.dt1;
        template.dt4 = entity1.dt4;

        Assert.assertTrue( serializer.match( ByteBuffer.wrap( data ), CacheStoreEntryWrapper.valueOf( bo, configuration, template ), false ) != null );

        template.dt2 = new Date( System.currentTimeMillis() );
        Assert
                .assertFalse( serializer.match( ByteBuffer.wrap( data ), CacheStoreEntryWrapper.valueOf( bo, configuration, template ), false ) != null );
    }

    @Test
    public void canMatchByTempateIfDoublesAndFloatsProvided() {
        ObjectBuffer objectBuffer = new ObjectBuffer( configuration.getKryo() );
        byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.valueOf( bo, configuration, entity1 ) );

        TestEntity1 template = new TestEntity1();
        template.cleanBeanProperties();

        template.fi1 = entity1.fi1;
        template.fi2 = entity1.fi2;
        template.s1 = entity1.s1;
        template.s4 = entity1.s4;
        template.l1 = entity1.l1;
        template.l4 = entity1.l4;
        template.dt1 = entity1.dt1;
        template.dt4 = entity1.dt4;
        template.f1 = entity1.f1;
        template.f4 = entity1.f4;
        template.d1 = entity1.d1;
        template.d4 = entity1.d4;

        Assert.assertTrue( serializer.match( ByteBuffer.wrap( data ), CacheStoreEntryWrapper.valueOf( bo, configuration, template ), false ) != null );

        template.d1 = -123.234234;
        Assert
                .assertFalse( serializer.match( ByteBuffer.wrap( data ), CacheStoreEntryWrapper.valueOf( bo, configuration, template ), false ) != null );
    }

    @Test
    public void canMatchByTempateIfDataProvided() {
        ObjectBuffer objectBuffer = new ObjectBuffer( configuration.getKryo() );
        byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.valueOf( bo, configuration, entity1 ) );

        TestEntity1 template = new TestEntity1();
        template.cleanBeanProperties();

        template.fi1 = entity1.fi1;
        template.fi2 = entity1.fi2;
        template.s1 = entity1.s1;
        template.s4 = entity1.s4;
        template.l1 = entity1.l1;
        template.l4 = entity1.l4;
        template.dt1 = entity1.dt1;
        template.dt4 = entity1.dt4;
        template.f1 = entity1.f1;
        template.f4 = entity1.f4;
        template.d1 = entity1.d1;
        template.d4 = entity1.d4;
        template.data1 = entity1.data1;

        Assert.assertTrue( serializer.match( ByteBuffer.wrap( data ), CacheStoreEntryWrapper.valueOf( bo, configuration, template ), false ) != null );

        template.data1 = Integer.valueOf( (int) System.currentTimeMillis() );
        Assert
                .assertFalse( serializer.match( ByteBuffer.wrap( data ), CacheStoreEntryWrapper.valueOf( bo, configuration, template ), false ) != null );
    }

    @Test
    public void tpsOverByteArray()
                                  throws InterruptedException {
        ObjectBuffer objectBuffer = new ObjectBuffer( configuration.getKryo() );
        final byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.valueOf( bo, configuration, entity1 ) );

        final TestEntity1 template = new TestEntity1();
        template.cleanBeanProperties();

        template.fi1 = entity1.fi1;
        template.fi2 = entity1.fi2;
        template.s1 = entity1.s1;
        template.s4 = entity1.s4;
        template.l1 = entity1.l1;
        template.l4 = entity1.l4;
        template.dt1 = entity1.dt1;
        template.dt4 = entity1.dt4;
        template.f1 = entity1.f1;
        template.f4 = entity1.f4;
        template.d1 = entity1.d1;
        template.d4 = entity1.d4;
        template.data1 = entity1.data1;

        long millis = System.currentTimeMillis();
        LinkedList<Throwable> errors = SpaceUtility.repeatConcurrently( THREADS_COUNT, ITERATIONS, new Runnable() {

            @Override
            public void run() {
                Assert.assertTrue( serializer.match( ByteBuffer.wrap( data ), CacheStoreEntryWrapper.valueOf( bo, configuration, template ), false ) != null );
            }
        } );
        Assert.assertTrue( errors.isEmpty() );

        logger.info( "tps over bytes {}", ITERATIONS / ( System.currentTimeMillis() - millis ) * 1000 );
    }

    @Test
    public void tpsOverByteArrayByUniqueIdentifier()
                                                    throws InterruptedException {

        ObjectBuffer objectBuffer = new ObjectBuffer( configuration.getKryo() );
        final byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.valueOf( bo, configuration, entity1 ) );

        final TestEntity1 template = new TestEntity1();
        template.cleanBeanProperties();
        template.uniqueIdentifier = entity1.getUniqueIdentifier();
        template.fi1 = entity1.fi1;
        template.fi2 = entity1.fi2;

        long millis = System.currentTimeMillis();
        LinkedList<Throwable> errors = SpaceUtility.repeatConcurrently( THREADS_COUNT, ITERATIONS, new Runnable() {

            @Override
            public void run() {
                Assert.assertTrue( serializer.match( ByteBuffer.wrap( data ), CacheStoreEntryWrapper.valueOf( bo, configuration, template ), true ) != null );
            }
        } );
        Assert.assertTrue( errors.isEmpty() );

        logger.info( "tps over bytes by id {}", ITERATIONS / ( System.currentTimeMillis() - millis ) * 1000 );
    }

    private static final int THREADS_COUNT = 50;
    private static final int ITERATIONS = 100000;
}
