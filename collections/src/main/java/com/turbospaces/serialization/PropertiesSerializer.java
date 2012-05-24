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
import java.util.ArrayList;

import org.springframework.data.mapping.PersistentProperty;

import com.esotericsoftware.kryo.serialize.FieldSerializer;
import com.turbospaces.model.BO;
import com.turbospaces.model.CacheStoreEntryWrapper;

/**
 * Optimized implementation of {@link FieldSerializer}, basically doing almost the same stuff, but with some
 * customization for reflection stuff and spring-data's properties bindings. </p>
 * 
 * This class allows you to do template matching over all fields and is compatible with type safe specifications.
 * 
 * @since 0.1
 */
@SuppressWarnings("rawtypes")
public final class PropertiesSerializer extends MatchingSerializer {
    private final BO entityMetadata;

    /**
     * create new properties serialized with the provided entity meta-data information.
     * 
     * @param kryo
     *            kryo serialization provider
     * @param entityMetadata
     *            class meta data provider
     */
    public PropertiesSerializer(final DecoratedKryo kryo, final BO entityMetadata) {
        super( kryo, new ArrayList<CachedSerializationProperty>( entityMetadata.getOrderedProperties().length ) {
            private static final long serialVersionUID = 1L;

            {
                PersistentProperty[] orderedProperties = entityMetadata.getOrderedProperties();
                for ( PersistentProperty orderedPropertie : orderedProperties )
                    add( new CachedSerializationProperty( orderedPropertie.getType() ) );
            }
        }.toArray( new CachedSerializationProperty[entityMetadata.getOrderedProperties().length] ) );
        this.entityMetadata = entityMetadata;
    }

    @Override
    public void write(final ByteBuffer buffer,
                      final Object object) {
        CacheStoreEntryWrapper cacheEntry = null;
        if ( !( object instanceof CacheStoreEntryWrapper ) )
            cacheEntry = CacheStoreEntryWrapper.writeValueOf( entityMetadata, object );

        Object[] bulkPropertyValues = ( cacheEntry == null ? (CacheStoreEntryWrapper) object : cacheEntry ).asPropertyValuesArray();
        for ( int i = 0, n = cachedProperties.length; i < n; i++ ) {
            CachedSerializationProperty cachedProperty = cachedProperties[i];
            Object value = bulkPropertyValues[i];
            writePropertyValue( cachedProperty, value, buffer );
        }
    }

    @Override
    public Object readID(final ByteBuffer buffer) {
        buffer.clear();
        final CachedSerializationProperty idProperty = cachedProperties[BO.getIdIndex()];
        Object id = readPropertyValue( idProperty, buffer );
        buffer.clear();
        return id;
    }

    @Override
    public Object read(final ByteBuffer buffer) {
        final Object values[] = new Object[cachedProperties.length];
        for ( int i = 0, n = cachedProperties.length; i < n; i++ )
            values[i] = readPropertyValue( cachedProperties[i], buffer );
        return entityMetadata.setBulkPropertyValues( entityMetadata.newInstance(), values );
    }

    /**
     * read entity from byte buffer stream (and remember the property values as array)
     * 
     * @param buffer
     *            byte array pointer
     * @return de-serialized entry
     */
    public SerializationEntry readToSerializedEntry(final ByteBuffer buffer) {
        buffer.clear();
        Object values[] = new Object[cachedProperties.length];
        for ( int i = 0, n = cachedProperties.length; i < n; i++ )
            values[i] = readPropertyValue( cachedProperties[i], buffer );
        SerializationEntry entry = new SerializationEntry(
                buffer,
                entityMetadata.setBulkPropertyValues( entityMetadata.newInstance(), values ),
                values );
        buffer.clear();
        return entry;
    }

    /**
     * @return entity meta-data
     */
    public BO getBO() {
        return entityMetadata;
    }

    @Override
    public Class getType() {
        return getBO().getOriginalPersistentEntity().getType();
    }
}
