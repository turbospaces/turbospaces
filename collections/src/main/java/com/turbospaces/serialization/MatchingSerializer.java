/**
 * Copyright (C) 2011-2012 Andrey Borisov <aandrey.borisov@gmail.com>
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

import com.esotericsoftware.kryo.serialize.SimpleSerializer;
import com.google.common.base.Preconditions;
import com.turbospaces.core.JVMUtil;
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
    private static boolean DEFAULT_BOOLEAN;
    private static byte DEFAULT_BYTE;
    private static short DEFAULT_SHORT;
    private static int DEFAULT_INT;
    private static long DEFAULT_LONG;
    private static float DEFAULT_FLOAT;
    private static double DEFAULT_DOUBLE;

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
            Object value = DecoratedKryo.readPropertyValue( kryo, cachedProperty, buffer );
            values[i] = value;
            Class<?> propertyType = cachedProperty.getPropertyType();

            if ( templateValue != null )
                if ( propertyType.isPrimitive() ) {
                    if ( propertyType.equals( boolean.class ) ) {
                        boolean b = (Boolean) templateValue;
                        if ( b != DEFAULT_BOOLEAN )
                            matches = ( b == ( (Boolean) templateValue ) );
                    }
                    else if ( propertyType.equals( byte.class ) ) {
                        byte b = (Byte) templateValue;
                        if ( b != DEFAULT_BYTE )
                            matches = ( b == ( (Byte) templateValue ) );
                    }
                    else if ( propertyType.equals( short.class ) ) {
                        short s = (Short) templateValue;
                        if ( s != DEFAULT_SHORT )
                            matches = ( s == ( (Short) templateValue ) );
                    }
                    else if ( propertyType.equals( int.class ) ) {
                        int ii = (Integer) templateValue;
                        if ( ii != DEFAULT_INT )
                            matches = ( ii == ( (Integer) templateValue ) );
                    }
                    else if ( propertyType.equals( long.class ) ) {
                        long l = (Long) templateValue;
                        if ( l != DEFAULT_LONG )
                            matches = ( l == ( (Long) templateValue ) );
                    }
                    else if ( propertyType.equals( float.class ) ) {
                        float f = (Float) templateValue;
                        if ( f != DEFAULT_FLOAT )
                            matches = ( f == ( (Float) templateValue ) );
                    }
                    else if ( propertyType.equals( double.class ) ) {
                        double d = (Double) templateValue;
                        if ( d != DEFAULT_DOUBLE )
                            matches = ( d == ( (Double) templateValue ) );
                    }
                }
                else
                    matches = JVMUtil.equals( templateValue, value );

            if ( !matches )
                break;
        }
        buffer.clear();
        return matches;
    }
}
