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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.mapping.model.BasicPersistentEntity;

import com.esotericsoftware.kryo.ObjectBuffer;
import com.google.common.base.Function;
import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.core.SpaceUtility;
import com.turbospaces.model.BO;
import com.turbospaces.model.TestEntity1;
import com.turbospaces.serialization.DefaultEntitySerializer;
import com.turbospaces.serialization.SerializationEntry;
import com.turbospaces.spaces.CacheStoreEntryWrapper;

@SuppressWarnings("javadoc")
public class DefaultEntitySerializerTest {
    TestEntity1 entity1;
    DefaultEntitySerializer serializer;
    SpaceConfiguration configuration;
    @SuppressWarnings("rawtypes")
    BO bo;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Before
    public void before()
                        throws Exception {
        entity1 = new TestEntity1();
        entity1.afterPropertiesSet();
        configuration = TestEntity1.configurationFor();
        serializer = new DefaultEntitySerializer( configuration );
        bo = new BO( (BasicPersistentEntity) configuration.getMappingContext().getPersistentEntity( TestEntity1.class ) );
    }

    @After
    public void after()
                       throws Exception {
        configuration.destroy();
    }

    @Test
    public void canPersist() {
        serializer.serialize( CacheStoreEntryWrapper.valueOf( bo, configuration, entity1 ), new ObjectBuffer( configuration.getKryo() ) );
    }

    @Test
    public void canDeserialize() {
        byte[] data = serializer
                .serialize( CacheStoreEntryWrapper.valueOf( bo, configuration, entity1 ), new ObjectBuffer( configuration.getKryo() ) );
        SerializationEntry entry = serializer.deserialize( ByteBuffer.wrap( data ), TestEntity1.class );
        entity1.assertMatch( (TestEntity1) entry.getObject() );
        assertThat( entry.getPropertyValues().length, greaterThan( 0 ) );
    }

    @Test
    public void canSerializeDesializeUnderStress()
                                                  throws InterruptedException {
        SpaceUtility.repeatConcurrently( 10, 10000, new Function<Integer, Object>() {

            @Override
            public Object apply(final Integer iteration) {
                TestEntity1 entity1 = new TestEntity1();
                entity1.afterPropertiesSet();
                byte[] data = serializer.serialize(
                        CacheStoreEntryWrapper.valueOf( bo, configuration, entity1 ),
                        new ObjectBuffer( configuration.getKryo() ) );
                TestEntity1 entity2 = (TestEntity1) serializer.deserialize( ByteBuffer.wrap( data ), TestEntity1.class ).getObject();
                entity1.assertMatch( entity2 );
                return this;
            }
        } );
    }
}
