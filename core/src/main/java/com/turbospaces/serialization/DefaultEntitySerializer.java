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

import java.nio.ByteBuffer;

import com.esotericsoftware.kryo.ObjectBuffer;
import com.esotericsoftware.kryo.SerializationException;
import com.esotericsoftware.kryo.Serializer;
import com.turbospaces.api.AbstractSpaceConfiguration;
import com.turbospaces.spaces.CacheStoreEntryWrapper;

/**
 * Default implementation of entity serializer. This one relies on high-performance kryo serialization library.
 * 
 * @since 0.1
 */
public class DefaultEntitySerializer implements EntitySerializer {
    private final AbstractSpaceConfiguration configuration;

    /**
     * create default entity serializer over configuration
     * 
     * NOTE: it is highly recommended to register persistent class with jspace before interacting, but still not
     * required.
     * 
     * @param configuration
     *            space configuration
     */
    public DefaultEntitySerializer(final AbstractSpaceConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public byte[] serialize(final CacheStoreEntryWrapper cacheEntry,
                            final ObjectBuffer objectBuffer) {
        return objectBuffer.writeObjectData( cacheEntry );

    }

    @Override
    public SerializationEntry deserialize(final ByteBuffer source,
                                          final Class<?> clazz)
                                                               throws SerializationException {
        source.clear();
        Serializer serializer = configuration.getKryo().getSerializer( clazz );
        return ( (PropertiesSerializer) serializer ).readToSerializedEntry( source );

    }

    @Override
    public Object matchByTemplate(final ByteBuffer source,
                                  final CacheStoreEntryWrapper cacheEntryTemplate,
                                  final boolean matchById)
                                                          throws SerializationException {
        source.clear();
        Serializer serializer = configuration.getKryo().getSerializer( cacheEntryTemplate.getPersistentEntity().getType() );
        return ( (PropertiesSerializer) serializer ).match( source, cacheEntryTemplate, matchById );
    }
}
