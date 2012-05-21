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

import org.springframework.data.mapping.PersistentProperty;
import org.springframework.util.ObjectUtils;

import com.esotericsoftware.kryo.Kryo.RegisteredClass;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serialize.FieldSerializer;
import com.esotericsoftware.kryo.serialize.SimpleSerializer;
import com.google.common.base.Preconditions;
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
public final class PropertiesSerializer extends SimpleSerializer {
    private final BO entityMetadata;
    private final DecoratedKryo kryo;
    private final CachedSerializationProperty[] cachedProperties;

    /**
     * create new properties serialized with the provided entity meta-data information.
     * 
     * @param kryo
     *            kryo serialization provider
     * @param entityMetadata
     *            class meta data provider
     */
    public PropertiesSerializer(final DecoratedKryo kryo, final BO entityMetadata) {
        this.entityMetadata = entityMetadata;
        this.kryo = Preconditions.checkNotNull( kryo );

        PersistentProperty[] orderedProperties = entityMetadata.getOrderedProperties();
        cachedProperties = new CachedSerializationProperty[orderedProperties.length];
        for ( int i = 0; i < orderedProperties.length; i++ )
            cachedProperties[i] = new CachedSerializationProperty( orderedProperties[i] );
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
            Serializer serializer = cachedProperty.getSerializer();

            if ( cachedProperty.isFinal() ) {
                if ( serializer == null )
                    cachedProperty.setSerializer( kryo.getRegisteredClass( cachedProperty.getPropertyType() ).getSerializer() );
                cachedProperty.write( buffer, value );
            }
            else {
                if ( value == null ) {
                    kryo.writeClass( buffer, null );
                    continue;
                }
                RegisteredClass registeredClass = kryo.writeClass( buffer, value.getClass() );

                if ( serializer == null )
                    serializer = registeredClass.getSerializer();
                serializer.writeObjectData( buffer, value );
            }
        }
    }

    /**
     * read the id property value from byte array(buffer) source
     * 
     * @param buffer
     *            byte array source
     * @return id property value
     */
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
        Object values[] = new Object[cachedProperties.length];
        for ( int i = 0, n = cachedProperties.length; i < n; i++ )
            values[i] = readPropertyValue( cachedProperties[i], buffer );
        return new SerializationEntry( buffer, entityMetadata.setBulkPropertyValues( entityMetadata.newInstance(), values ), values );
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
     *            matching template
     * @param buffer
     *            byte array buffer
     * @return matched or not
     */
    public boolean match(final ByteBuffer buffer,
                         final CacheStoreEntryWrapper cacheEntryTemplate) {
        Object[] values = new Object[cachedProperties.length];
        Object[] templateValues = cacheEntryTemplate.asPropertyValuesArray();
        boolean matches = true;
        for ( int i = 0, n = cachedProperties.length; i < n; i++ ) {
            CachedSerializationProperty cachedProperty = cachedProperties[i];
            Object templateValue = templateValues[i];
            Object value = readPropertyValue( cachedProperty, buffer );
            values[i] = value;

            if ( templateValue != null && !ObjectUtils.nullSafeEquals( templateValue, value ) ) {
                matches = false;
                break;
            }
        }
        return matches;
    }

    @SuppressWarnings("unchecked")
    private Object readPropertyValue(final CachedSerializationProperty cachedProperty,
                                     final ByteBuffer buffer) {
        Object value = null;
        Serializer serializer = cachedProperty.getSerializer();
        if ( cachedProperty.isFinal() ) {
            if ( serializer == null )
                cachedProperty.setSerializer( kryo.getRegisteredClass( cachedProperty.getPropertyType() ).getSerializer() );
            value = cachedProperty.read( buffer, cachedProperty.getPropertyType() );
        }
        else {
            RegisteredClass registeredClass = kryo.readClass( buffer );
            if ( registeredClass != null ) {
                if ( serializer == null )
                    serializer = registeredClass.getSerializer();
                value = serializer.readObjectData( buffer, registeredClass.getType() );
            }
        }
        return value;
    }

    /**
     * @return entity meta-data
     */
    public BO getBO() {
        return entityMetadata;
    }
}
