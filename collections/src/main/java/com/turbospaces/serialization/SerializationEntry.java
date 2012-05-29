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

import javax.annotation.concurrent.Immutable;

/**
 * serialization/de-serialization entry wrapper. holder for de-serialized object and property values of the object.
 * 
 * @since 0.1
 */
@Immutable
public final class SerializationEntry {
    private final ByteBuffer src;
    private final Object object;
    private final Object[] propertyValues;

    /**
     * create new instance as associate original source(byte buffer)
     * 
     * @param src
     *            byte buffer source
     * @param object
     *            actual bean
     * @param propertyValues
     *            array of property values derived from bean
     */
    public SerializationEntry(final ByteBuffer src, final Object object, final Object[] propertyValues) {
        super();
        this.src = src;
        this.object = object;
        this.propertyValues = propertyValues;
    }

    /**
     * @return bean itself
     */
    public Object getObject() {
        return object;
    }

    /**
     * @return bean's property values
     */
    public Object[] getPropertyValues() {
        return propertyValues;
    }

    /**
     * @return the original byte buffer source
     */
    public ByteBuffer getSrc() {
        return src;
    }
}
