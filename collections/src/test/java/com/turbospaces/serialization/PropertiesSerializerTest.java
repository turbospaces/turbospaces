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
import java.util.List;
import java.util.UUID;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.ObjectBuffer;
import com.turbospaces.core.JVMUtil;
import com.turbospaces.model.BO;
import com.turbospaces.model.CacheStoreEntryWrapper;
import com.turbospaces.model.TestEntity1;

@SuppressWarnings("javadoc")
public class PropertiesSerializerTest {
    Logger logger = LoggerFactory.getLogger( getClass() );
    DecoratedKryo kryo;
    TestEntity1 entity1;
    BO bo;
    PropertiesSerializer serializer;

    @Before
    public void before()
                        throws Exception {
        bo = TestEntity1.getPersistentEntity();
        kryo = new DecoratedKryo();
        BO.registerPersistentClasses( kryo, bo.getOriginalPersistentEntity() );
        serializer = new PropertiesSerializer( kryo, bo );

        entity1 = new TestEntity1();
        entity1.afterPropertiesSet();

        JVMUtil.gc();
    }

    @Test
    public void canSerializeEntity() {
        ObjectBuffer objectBuffer = new ObjectBuffer( kryo );
        byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ) );
        logger.info( "wrote {}.({} bytes) to {}", new Object[] { entity1, data.length, Arrays.toString( data ) } );
    }

    @Test
    public void canSerializeDirecyEntity() {
        TestEntity1 entity1 = new TestEntity1();
        entity1.afterPropertiesSet();
        ObjectBuffer objectBuffer = new ObjectBuffer( kryo );
        byte[] data = objectBuffer.writeObjectData( entity1 );
        TestEntity1 clone = objectBuffer.readObjectData( data, entity1.getClass() );
        clone.assertMatch( entity1 );
    }

    @Test
    public void canReadIdProperty() {
        ObjectBuffer objectBuffer = new ObjectBuffer( kryo );
        byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ) );
        assertThat( serializer.readID( ByteBuffer.wrap( data ) ), is( notNullValue() ) );
    }

    @Test
    public void canDeSerializeEntity() {
        ObjectBuffer objectBuffer = new ObjectBuffer( kryo );
        byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ) );
        TestEntity1 data2 = objectBuffer.readObjectData( data, TestEntity1.class );
        entity1.assertMatch( data2 );
    }

    @Test
    public void canMatchByTempateIfNoFieldProvidedExceptPrimitives() {
        ObjectBuffer objectBuffer = new ObjectBuffer( kryo );
        byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ) );

        TestEntity1 template = new TestEntity1();
        template.cleanBeanProperties();

        template.fi1 = entity1.fi1;
        template.fi2 = entity1.fi2;

        CacheStoreEntryWrapper cacheStoreEntryWrapper = CacheStoreEntryWrapper.writeValueOf( bo, template );
        Assert.assertTrue( serializer.matches( ByteBuffer.wrap( data ), cacheStoreEntryWrapper ) );
    }

    @Test
    public void cantMatchByTemplateIfPrimitivesDidntMatch() {
        ObjectBuffer objectBuffer = new ObjectBuffer( kryo );
        byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ) );

        TestEntity1 template = new TestEntity1();
        template.cleanBeanProperties();

        CacheStoreEntryWrapper cacheStoreEntryWrapper = CacheStoreEntryWrapper.writeValueOf( bo, template );
        Assert.assertFalse( serializer.matches( ByteBuffer.wrap( data ), cacheStoreEntryWrapper ) );
    }

    @Test
    public void canMatchByTempateIfPrimaryKeyProvided() {
        ObjectBuffer objectBuffer = new ObjectBuffer( kryo );
        byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ) );

        TestEntity1 template = new TestEntity1();
        template.cleanBeanProperties();

        template.fi1 = entity1.fi1;
        template.fi2 = entity1.fi2;
        template.uniqueIdentifier = entity1.getUniqueIdentifier();

        CacheStoreEntryWrapper cacheStoreEntryWrapper = CacheStoreEntryWrapper.writeValueOf( bo, template );
        Assert.assertTrue( serializer.matches( ByteBuffer.wrap( data ), cacheStoreEntryWrapper ) );
    }

    @Test
    public void canMatchByTempateIfPrimaryKeyAndVersionProvided() {
        entity1.optimisticLockVersion = 123;
        ObjectBuffer objectBuffer = new ObjectBuffer( kryo );
        byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ) );

        TestEntity1 template = new TestEntity1();
        template.cleanBeanProperties();

        template.fi1 = entity1.fi1;
        template.fi2 = entity1.fi2;
        template.uniqueIdentifier = entity1.getUniqueIdentifier();
        template.optimisticLockVersion = entity1.getOptimisticLockVersion();

        CacheStoreEntryWrapper cacheStoreEntryWrapper = CacheStoreEntryWrapper.writeValueOf( bo, template );
        Assert.assertTrue( serializer.matches( ByteBuffer.wrap( data ), cacheStoreEntryWrapper ) );
    }

    @Test
    public void canMatchByTempateIfStringProvided() {
        ObjectBuffer objectBuffer = new ObjectBuffer( kryo );
        byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ) );

        TestEntity1 template = new TestEntity1();
        template.cleanBeanProperties();

        template.fi1 = entity1.fi1;
        template.fi2 = entity1.fi2;
        template.s1 = entity1.s1;
        template.s4 = entity1.s4;

        Assert.assertTrue( serializer.matches( ByteBuffer.wrap( data ), CacheStoreEntryWrapper.writeValueOf( bo,

        template ) ) );
        template.s2 = UUID.randomUUID().toString();
        Assert.assertFalse( serializer.matches( ByteBuffer.wrap( data ), CacheStoreEntryWrapper.writeValueOf( bo,

        template ) ) );
    }

    @Test
    public void canMatchByTempateIfLongsProvided() {
        ObjectBuffer objectBuffer = new ObjectBuffer( kryo );
        byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ) );

        TestEntity1 template = new TestEntity1();
        template.cleanBeanProperties();

        template.fi1 = entity1.fi1;
        template.fi2 = entity1.fi2;
        template.s1 = entity1.s1;
        template.s4 = entity1.s4;
        template.l1 = entity1.l1;
        template.l4 = entity1.l4;

        Assert.assertTrue( serializer.matches( ByteBuffer.wrap( data ), CacheStoreEntryWrapper.writeValueOf( bo, template ) ) );

        template.l2 = System.currentTimeMillis();
        Assert.assertFalse( serializer.matches( ByteBuffer.wrap( data ), CacheStoreEntryWrapper.writeValueOf( bo, template ) ) );
    }

    @Test
    public void canMatchByTempateIfDatesProvided() {
        ObjectBuffer objectBuffer = new ObjectBuffer( kryo );
        byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ) );

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

        Assert.assertTrue( serializer.matches( ByteBuffer.wrap( data ), CacheStoreEntryWrapper.writeValueOf( bo,

        template ) ) );

        template.dt2 = new Date( System.currentTimeMillis() );
        Assert.assertFalse( serializer.matches( ByteBuffer.wrap( data ), CacheStoreEntryWrapper.writeValueOf( bo,

        template ) ) );
    }

    @Test
    public void canMatchByTempateIfDoublesAndFloatsProvided() {
        ObjectBuffer objectBuffer = new ObjectBuffer( kryo );
        byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ) );

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

        Assert.assertTrue( serializer.matches( ByteBuffer.wrap( data ), CacheStoreEntryWrapper.writeValueOf( bo, template ) ) );

        template.d1 = -123.234234;
        Assert.assertFalse( serializer.matches( ByteBuffer.wrap( data ), CacheStoreEntryWrapper.writeValueOf( bo, template ) ) );
    }

    @Test
    public void canMatchByTempateIfDataProvided() {
        ObjectBuffer objectBuffer = new ObjectBuffer( kryo );
        byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ) );

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

        Assert.assertTrue( serializer.matches( ByteBuffer.wrap( data ), CacheStoreEntryWrapper.writeValueOf( bo, template ) ) );
        template.data1 = Integer.valueOf( (int) System.currentTimeMillis() );
        Assert.assertFalse( serializer.matches( ByteBuffer.wrap( data ), CacheStoreEntryWrapper.writeValueOf( bo, template ) ) );
    }

    @Test
    public void tpsOverByteArray() {
        ObjectBuffer objectBuffer = new ObjectBuffer( kryo );
        final byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ) );

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
        List<Throwable> errors = JVMUtil.repeatConcurrently( THREADS_COUNT, ITERATIONS, new Runnable() {

            @Override
            public void run() {
                Assert.assertTrue( serializer.matches( ByteBuffer.wrap( data ), CacheStoreEntryWrapper.writeValueOf( bo, template ) ) );
            }
        } );
        Assert.assertTrue( errors.isEmpty() );

        logger.info( "tps over bytes {}", ITERATIONS / ( System.currentTimeMillis() - millis ) * 1000 );
    }

    @Test
    public void tpsOverByteArrayByUniqueIdentifier() {

        ObjectBuffer objectBuffer = new ObjectBuffer( kryo );
        final byte[] data = objectBuffer.writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ) );

        final TestEntity1 template = new TestEntity1();
        template.cleanBeanProperties();
        template.uniqueIdentifier = entity1.getUniqueIdentifier();
        template.fi1 = entity1.fi1;
        template.fi2 = entity1.fi2;

        long millis = System.currentTimeMillis();
        List<Throwable> errors = JVMUtil.repeatConcurrently( THREADS_COUNT, ITERATIONS, new Runnable() {

            @Override
            public void run() {
                Assert.assertTrue( serializer.matches( ByteBuffer.wrap( data ), CacheStoreEntryWrapper.writeValueOf( bo, template ) ) );
            }
        } );
        Assert.assertTrue( errors.isEmpty() );

        logger.info( "tps over bytes by id {}", ITERATIONS / ( System.currentTimeMillis() - millis ) * 1000 );
    }

    private static final int THREADS_COUNT = 50;
    private static final int ITERATIONS = 100000;
}
