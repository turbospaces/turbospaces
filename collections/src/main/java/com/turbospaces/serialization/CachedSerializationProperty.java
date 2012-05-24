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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;

/**
 * Controls how a property/field will be serialized. This class needs to be considered as library internal.
 * 
 * @since 0.1
 */
final class CachedSerializationProperty {
    private final Class<?> type;
    private Serializer serializer;
    private final boolean canBeNull, isFinal;

    /**
     * create cached serialization binding for particular persistent property.
     * 
     * @param type
     *            The concrete persistent property. The serializer registered by default {@link EntitySerializer} for
     *            the specified class will be used. Only set to a non-null value if the property type
     *            in the class definition is final or the values for this field will not vary.
     */
    CachedSerializationProperty(final Class<?> type) {
        this.type = type;
        this.canBeNull = !type.isPrimitive();
        this.isFinal = Kryo.isFinal( type );
    }

    /**
     * associate kryo serializer with this field.
     * 
     * @param serializer
     *            kryo serializer reference
     */
    void setSerializer(final Serializer serializer) {
        this.serializer = serializer;
    }

    /**
     * @return true if this field's value can be null(meaning it is not primitive).
     */
    public boolean canBeNull() {
        return canBeNull;
    }

    /**
     * @return true if the type of associated field is final (meaning that there is no need to write which exact field
     *         value's class is serialized and of course saves bytes).
     */
    public boolean isFinal() {
        return isFinal;
    }

    /**
     * @return field's value property class.
     */
    public Class<?> getPropertyType() {
        return type;
    }

    /**
     * @return kryo serializer associated with this field.
     */
    public Serializer getSerializer() {
        return serializer;
    }

    /**
     * write given value to byte buffer array, optimize for primitive.
     * 
     * @param buffer
     * @param value
     */
    void write(final ByteBuffer buffer,
               final Object value) {
        if ( canBeNull() )
            serializer.writeObject( buffer, value );
        else
            serializer.writeObjectData( buffer, value );
    }

    /**
     * read(de-serialize) object's data from byte buffer array.
     * 
     * @param buffer
     * @param concreteType
     * @return de-serialized data
     */
    Object read(final ByteBuffer buffer,
                final Class<?> concreteType) {
        return canBeNull() ? getSerializer().readObject( buffer, concreteType ) : getSerializer().readObjectData( buffer, concreteType );
    }
}
