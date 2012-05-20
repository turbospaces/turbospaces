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
import static org.hamcrest.Matchers.greaterThan;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.esotericsoftware.kryo.ObjectBuffer;
import com.google.common.base.Function;
import com.turbospaces.core.JVMUtil;
import com.turbospaces.model.BO;
import com.turbospaces.model.CacheStoreEntryWrapper;
import com.turbospaces.model.TestEntity1;

@SuppressWarnings("javadoc")
public class DefaultEntitySerializerTest {
    TestEntity1 entity1;
    BO bo;
    DecoratedKryo kryo;

    @Before
    public void before()
                        throws Exception {
        bo = TestEntity1.getPersistentEntity();
        kryo = new DecoratedKryo();
        kryo.registerPersistentClasses( bo.getOriginalPersistentEntity() );
        entity1 = new TestEntity1();
        entity1.afterPropertiesSet();
    }

    @Test
    public void canPersist() {
        new ObjectBuffer( kryo ).writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ) );
    }

    @Test
    public void canDeserialize() {
        byte[] data = new ObjectBuffer( kryo ).writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ) );
        SerializationEntry entry = kryo.deserialize( ByteBuffer.wrap( data ), TestEntity1.class );
        entity1.assertMatch( (TestEntity1) entry.getObject() );
        assertThat( entry.getPropertyValues().length, greaterThan( 0 ) );
    }

    @Test
    public void canSerializeDesializeUnderStress() {
        Assert.assertEquals( JVMUtil.repeatConcurrently( 10, 10000, new Function<Integer, Object>() {
            @Override
            public Object apply(final Integer iteration) {
                TestEntity1 entity1 = new TestEntity1();
                entity1.afterPropertiesSet();
                byte[] data = new ObjectBuffer( kryo ).writeObjectData( CacheStoreEntryWrapper.writeValueOf( bo, entity1 ) );
                TestEntity1 entity2 = (TestEntity1) kryo.deserialize( ByteBuffer.wrap( data ), TestEntity1.class ).getObject();
                entity1.assertMatch( entity2 );
                return this;
            }
        } ).isEmpty(), true );
    }
}
