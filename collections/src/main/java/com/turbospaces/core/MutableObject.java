package com.turbospaces.core;

import java.util.concurrent.atomic.AtomicReference;

/**
 * The same as <a
 * href="http://commons.apache.org/lang/api-2.4/org/apache/commons/lang/mutable/MutableObject.html">Apache Commons
 * Mutable Object</a> and can be used with anonymous inner classes instead of {@link AtomicReference} or one-element
 * array.
 * 
 * @param <V>
 *            type of value
 * 
 * @since 0.1
 */
public class MutableObject<V> {
    private V value;

    @SuppressWarnings("javadoc")
    public void set(final V newval) {
        value = newval;
    }

    @SuppressWarnings("javadoc")
    public V get() {
        return value;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if ( obj instanceof MutableObject )
            return value.equals( obj );
        return super.equals( obj );
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
