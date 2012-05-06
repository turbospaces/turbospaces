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

import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serialize.IntSerializer;
import com.turbospaces.spaces.AbstractJSpace;

/**
 * optimized version of multi-dimension array suitable for better network communications(better serialization of data
 * needs to be transfered).
 * 
 * @see AbstractJSpace
 * @since 0.1
 */
public class NetworkCommunicationSerializer extends Serializer {

    @Override
    public void writeObjectData(final ByteBuffer buffer,
                                final Object object) {
        byte[][] arr = (byte[][]) object;
        IntSerializer.put( buffer, arr.length, true );

        for ( byte[] bytes : arr ) {
            IntSerializer.put( buffer, bytes.length, true );
            buffer.put( bytes );
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T readObjectData(final ByteBuffer buffer,
                                final Class<T> type) {
        int length = IntSerializer.get( buffer, true );
        byte[][] arr = new byte[length][];

        for ( int i = 0; i < length; i++ ) {
            byte[] b = new byte[IntSerializer.get( buffer, true )];
            buffer.get( b );
            arr[i] = b;
        }

        return (T) arr;
    }
}
