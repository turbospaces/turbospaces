package com.turbospaces.serialization;

import java.nio.ByteBuffer;

import com.esotericsoftware.kryo.serialize.SimpleSerializer;
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

    /**
     * read the id property value from byte array(buffer) source in a fast manner.
     * 
     * @param buffer
     *            byte array source
     * @return id property value
     */
    public abstract Object readID(ByteBuffer buffer);

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
    public abstract boolean match(final ByteBuffer buffer,
                                  final CacheStoreEntryWrapper cacheEntryTemplate);

    /**
     * @return the type of entity this serialized capable to work with.
     */
    public abstract Class<?> getType();
}
