package com.turbospaces.serialization;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.Immutable;

/**
 * serialization/de-serialization entry wrapper. holder for de-serialized object and property values of the object.
 * 
 * @since 0.1
 */
@Immutable
public class SerializationEntry {
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
