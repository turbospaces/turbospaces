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
package com.elsecloud.serialization;

import java.nio.ByteBuffer;

import org.springframework.data.mapping.PersistentProperty;
import org.springframework.util.ObjectUtils;

import com.elsecloud.api.AbstractSpaceConfiguration;
import com.elsecloud.model.BO;
import com.elsecloud.spaces.CacheStoreEntryWrapper;
import com.esotericsoftware.kryo.Kryo.RegisteredClass;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serialize.FieldSerializer;
import com.esotericsoftware.kryo.serialize.SimpleSerializer;

/**
 * Optimized implementation of {@link FieldSerializer}, basically doing almost the same stuff, but with some
 * customization for reflection stuff and spring-data's properties bindings. </p>
 * 
 * This class allows you to do template matching over all fields and is compatible with type safe specifications.
 * 
 * @since 0.1
 */
@SuppressWarnings("rawtypes")
public final class PropertiesSerializer extends SimpleSerializer {
    private final AbstractSpaceConfiguration configuration;
    private final BO entityMetadata;
    private final CachedSerializationProperty[] cachedProperties;

    @SuppressWarnings({ "javadoc" })
    public PropertiesSerializer(final AbstractSpaceConfiguration configuration, final BO<?, ?> entityMetadata) {
        this.configuration = configuration;
        this.entityMetadata = entityMetadata;

        {
            PersistentProperty[] orderedProperties = entityMetadata.getOrderedProperties();
            cachedProperties = new CachedSerializationProperty[orderedProperties.length];
            for ( int i = 0; i < orderedProperties.length; i++ )
                cachedProperties[i] = new CachedSerializationProperty( orderedProperties[i] );
        }
    }

    @Override
    public void write(final ByteBuffer buffer,
                      final Object object) {
        CacheStoreEntryWrapper cacheEntry = null;
        if ( !( object instanceof CacheStoreEntryWrapper ) )
            cacheEntry = CacheStoreEntryWrapper.valueOf( entityMetadata, configuration, object );

        try {
            final Object[] bulkPropertyValues = ( cacheEntry == null ? (CacheStoreEntryWrapper) object : cacheEntry ).asPropertyValuesArray();
            for ( int i = 0, n = cachedProperties.length; i < n; i++ ) {
                CachedSerializationProperty cachedProperty = cachedProperties[i];
                Object value = bulkPropertyValues[i];
                Serializer serializer = cachedProperty.getSerializer();

                if ( cachedProperty.isFinal() ) {
                    if ( serializer == null )
                        cachedProperty.setSerializer( configuration.getKryo().getRegisteredClass( cachedProperty.getPropertyType() ).getSerializer() );
                    cachedProperty.write( buffer, value );
                }
                else {
                    if ( value == null ) {
                        configuration.getKryo().writeClass( buffer, null );
                        continue;
                    }
                    RegisteredClass registeredClass = configuration.getKryo().writeClass( buffer, value.getClass() );

                    if ( serializer == null )
                        serializer = registeredClass.getSerializer();
                    serializer.writeObjectData( buffer, value );
                }
            }
        }
        finally {
            if ( cacheEntry != null )
                CacheStoreEntryWrapper.recycle( cacheEntry );
        }
    }

    /**
     * read the id property value over byte source
     * 
     * @param buffer
     * @return id property value
     */
    public Object readID(final ByteBuffer buffer) {
        buffer.clear();
        final CachedSerializationProperty idProperty = cachedProperties[BO.getIdIndex()];
        return readPropertyValue( idProperty, buffer );
    }

    @Override
    public Object read(final ByteBuffer buffer) {
        final Object values[] = new Object[cachedProperties.length];
        for ( int i = 0, n = cachedProperties.length; i < n; i++ )
            values[i] = readPropertyValue( cachedProperties[i], buffer );

        return entityMetadata.setBulkPropertyValues( entityMetadata.newInstance(), values, configuration.getConversionService() );
    }

    /**
     * read entity from byte buffer stream (and remember the property values as array)
     * 
     * @param buffer
     * @return de-serialized entry
     */
    public SerializationEntry readToSerializedEntry(final ByteBuffer buffer) {
        final Object values[] = new Object[cachedProperties.length];
        for ( int i = 0, n = cachedProperties.length; i < n; i++ )
            values[i] = readPropertyValue( cachedProperties[i], buffer );

        Object object = entityMetadata.setBulkPropertyValues( entityMetadata.newInstance(), values, configuration.getConversionService() );
        return new SerializationEntry( object, values );
    }

    /**
     * check whether entitie's content stored in byte buffer array matches with template object. this is classical 'find
     * by example' method operating on low-level byte array buffer. </p>
     * 
     * if the match is happening, then construct new entity and return shallow copy of matched entity.</p>
     * 
     * <strong>NOTE:</strong> you should be very careful with primitive types and default values as this it not handled
     * at the time at all.
     * 
     * @param cacheEntryTemplate
     * @param buffer
     * @param matchById
     *            match only by id?
     * 
     * @return matched object if any (or <code>null</code>)
     */
    public Object match(final ByteBuffer buffer,
                        final CacheStoreEntryWrapper cacheEntryTemplate,
                        final boolean matchById) {
        final Object[] values = new Object[cachedProperties.length];
        final Object[] templateValues = cacheEntryTemplate.asPropertyValuesArray();

        boolean matches = true;
        for ( int i = 0, n = cachedProperties.length; i < n; i++ ) {
            final CachedSerializationProperty cachedProperty = cachedProperties[i];
            final Object templateValue = templateValues[i];

            Object value = readPropertyValue( cachedProperty, buffer );
            values[i] = value;

            if ( templateValue != null && !ObjectUtils.nullSafeEquals( templateValue, value ) ) {
                matches = false;
                break;
            }
        }

        if ( matches ) {
            Object instance = entityMetadata.newInstance();
            return entityMetadata.setBulkPropertyValues( instance, values, configuration.getConversionService() );
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    private Object readPropertyValue(final CachedSerializationProperty cachedProperty,
                                     final ByteBuffer buffer) {
        Object value = null;
        Serializer serializer = cachedProperty.getSerializer();

        if ( cachedProperty.isFinal() ) {
            if ( serializer == null )
                cachedProperty.setSerializer( configuration.getKryo().getRegisteredClass( cachedProperty.getPropertyType() ).getSerializer() );
            value = cachedProperty.read( buffer, cachedProperty.getPropertyType() );
        }
        else {
            final RegisteredClass registeredClass = configuration.getKryo().readClass( buffer );
            if ( registeredClass != null ) {
                if ( serializer == null )
                    serializer = registeredClass.getSerializer();
                value = serializer.readObjectData( buffer, registeredClass.getType() );
            }
        }

        return value;
    }
}
