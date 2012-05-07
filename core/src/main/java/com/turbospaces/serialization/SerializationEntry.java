package com.turbospaces.serialization;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.Immutable;

/**
 * serialization/de-serialization entry wrapper. holder for de-serialized object and property values of the object.
 * 
 * @since 0.1
 */
@SuppressWarnings("javadoc")
@Immutable
public class SerializationEntry {
    private final ByteBuffer src;
    private final Object object;
    private final Object[] propertyValues;

    public SerializationEntry(final ByteBuffer src, final Object object, final Object[] propertyValues) {
        super();
        this.src = src;
        this.object = object;
        this.propertyValues = propertyValues;
    }

    public Object getObject() {
        return object;
    }

    public Object[] getPropertyValues() {
        return propertyValues;
    }

    public ByteBuffer getSrc() {
        return src;
    }
}
