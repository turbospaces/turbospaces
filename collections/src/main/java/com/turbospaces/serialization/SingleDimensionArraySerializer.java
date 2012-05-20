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

import java.lang.reflect.Array;
import java.nio.ByteBuffer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Kryo.RegisteredClass;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serialize.IntSerializer;
import com.esotericsoftware.kryo.serialize.LongSerializer;
import com.esotericsoftware.kryo.serialize.ShortSerializer;

/**
 * faster version of array serializer optimized for single dimension array(which is used in most cases).
 * 
 * @since 0.1
 */
public class SingleDimensionArraySerializer extends Serializer {
    private final boolean isPrimitive;
    private final Kryo kryo;

    /**
     * create new single dimension serializer over original array serializer
     * 
     * @param type
     *            array[] type
     * @param kryo
     *            kryo serializer
     */
    public SingleDimensionArraySerializer(final Class<?> type, final Kryo kryo) {
        super();

        this.kryo = kryo;
        this.isPrimitive = type.getComponentType().isPrimitive();
    }

    @Override
    public void writeObjectData(final ByteBuffer buffer,
                                final Object object) {
        final Class<? extends Object> elementType = object.getClass().getComponentType();
        if ( isPrimitive ) {
            if ( elementType == int.class ) {
                int[] arr = (int[]) object;
                IntSerializer.put( buffer, arr.length, true );
                for ( int element : arr )
                    IntSerializer.put( buffer, element, true );
            }
            else if ( elementType == long.class ) {
                long[] arr = (long[]) object;
                IntSerializer.put( buffer, arr.length, true );
                for ( long element : arr )
                    LongSerializer.put( buffer, element, true );
            }
            else if ( elementType == char.class ) {
                char[] arr = (char[]) object;
                IntSerializer.put( buffer, arr.length, true );
                for ( char element : arr )
                    buffer.putChar( element );
            }
            else if ( elementType == byte.class ) {
                byte[] arr = (byte[]) object;
                IntSerializer.put( buffer, arr.length, true );
                for ( byte element : arr )
                    buffer.put( element );
            }
            else if ( elementType == boolean.class ) {
                boolean[] arr = (boolean[]) object;
                IntSerializer.put( buffer, arr.length, true );
                for ( boolean element : arr )
                    buffer.put( (byte) ( element ? 1 : 0 ) );
            }
            else if ( elementType == float.class ) {
                float[] arr = (float[]) object;
                IntSerializer.put( buffer, arr.length, true );
                for ( float element : arr )
                    buffer.putFloat( element );
            }
            else if ( elementType == double.class ) {
                double[] arr = (double[]) object;
                IntSerializer.put( buffer, arr.length, true );
                for ( double element : arr )
                    buffer.putDouble( element );
            }
            else if ( elementType == short.class ) {
                short[] arr = (short[]) object;
                IntSerializer.put( buffer, arr.length, true );
                for ( short element : arr )
                    ShortSerializer.put( buffer, element, true );
            }
        }
        else {
            Object[] arr = (Object[]) object;
            IntSerializer.put( buffer, arr.length, true );
            Class<?> componentType = object.getClass().getComponentType();
            RegisteredClass registeredClass = kryo.getRegisteredClass( componentType );
            Serializer elementSerializer = registeredClass.getSerializer();
            for ( Object element : arr )
                elementSerializer.writeObject( buffer, element );
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T readObjectData(final ByteBuffer buffer,
                                final Class<T> type) {
        int length = IntSerializer.get( buffer, true );

        if ( isPrimitive )
            if ( type == int[].class ) {
                int[] result = new int[length];
                for ( int i = 0; i < length; i++ )
                    result[i] = IntSerializer.get( buffer, true );
                return (T) result;
            }
            else if ( type == long[].class ) {
                long[] result = new long[length];
                for ( int i = 0; i < length; i++ )
                    result[i] = LongSerializer.get( buffer, true );
                return (T) result;
            }
            else if ( type == char[].class ) {
                char[] result = new char[length];
                for ( int i = 0; i < length; i++ )
                    result[i] = buffer.getChar();
                return (T) result;
            }
            else if ( type == byte[].class ) {
                byte[] result = new byte[length];
                for ( int i = 0; i < length; i++ )
                    result[i] = buffer.get();
                return (T) result;
            }
            else if ( type == boolean[].class ) {
                boolean[] result = new boolean[length];
                for ( int i = 0; i < length; i++ )
                    result[i] = buffer.get() == 1 ? true : false;
                return (T) result;
            }
            else if ( type == float[].class ) {
                float[] result = new float[length];
                for ( int i = 0; i < length; i++ )
                    result[i] = buffer.getFloat();
                return (T) result;
            }
            else if ( type == double[].class ) {
                double[] result = new double[length];
                for ( int i = 0; i < length; i++ )
                    result[i] = buffer.getDouble();
                return (T) result;
            }
            else if ( type == short[].class ) {
                short[] result = new short[length];
                for ( int i = 0; i < length; i++ )
                    result[i] = ShortSerializer.get( buffer, true );
                return (T) result;
            }

        Class<?> componentType = type.getComponentType();
        RegisteredClass registeredClass = kryo.getRegisteredClass( componentType );
        Serializer elementSerializer = registeredClass.getSerializer();
        T array = (T) Array.newInstance( componentType, length );
        for ( int i = 0; i < length; i++ )
            Array.set( array, i, elementSerializer.readObject( buffer, componentType ) );
        return array;
    }
}
