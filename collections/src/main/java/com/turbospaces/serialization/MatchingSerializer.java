package com.turbospaces.serialization;

import java.nio.ByteBuffer;

import org.springframework.util.ObjectUtils;

import com.esotericsoftware.kryo.Kryo.RegisteredClass;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serialize.SimpleSerializer;
import com.google.common.base.Preconditions;
import com.turbospaces.model.CacheStoreEntryWrapper;

/**
 * marker class - meaning that this serialized understands the concept of ID and can read the id property from buffer in
 * a fast manner as well as perform matching(like java spaces template matching).
 * 
 * @since 0.1
 * 
 * @param <V>
 *            value type
 */
public abstract class MatchingSerializer<V> extends SimpleSerializer<V> {
    final DecoratedKryo kryo;
    final CachedSerializationProperty[] cachedProperties;

    MatchingSerializer(final DecoratedKryo kryo, final CachedSerializationProperty[] cachedProperties) {
        this.kryo = Preconditions.checkNotNull( kryo );
        this.cachedProperties = Preconditions.checkNotNull( cachedProperties );
    }

    /**
     * read the id property value from byte array(buffer) source in a fast manner.
     * 
     * @param buffer
     *            byte array source
     * @return id property value
     */
    public abstract Object readID(ByteBuffer buffer);

    /**
     * @return the type of entity this serialized capable to work with.
     */
    public abstract Class<?> getType();

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
    public final boolean matches(final ByteBuffer buffer,
                                 final CacheStoreEntryWrapper cacheEntryTemplate) {
        buffer.clear();
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
        buffer.clear();
        return matches;
    }

    final void writePropertyValue(final CachedSerializationProperty cachedProperty,
                                  final Object value,
                                  final ByteBuffer buffer) {
        Serializer serializer = cachedProperty.getSerializer();

        if ( cachedProperty.isFinal() ) {
            if ( serializer == null )
                cachedProperty.setSerializer( kryo.getRegisteredClass( cachedProperty.getPropertyType() ).getSerializer() );
            cachedProperty.write( buffer, value );
        }
        else {
            if ( value == null ) {
                kryo.writeClass( buffer, null );
                return;
            }
            RegisteredClass registeredClass = kryo.writeClass( buffer, value.getClass() );

            if ( serializer == null )
                serializer = registeredClass.getSerializer();
            serializer.writeObjectData( buffer, value );
        }
    }

    @SuppressWarnings("unchecked")
    final Object readPropertyValue(final CachedSerializationProperty cachedProperty,
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
}
