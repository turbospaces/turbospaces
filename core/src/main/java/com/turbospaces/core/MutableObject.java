package com.turbospaces.core;

/**
 * The same as <a
 * href="http://commons.apache.org/lang/api-2.4/org/apache/commons/lang/mutable/MutableObject.html">Apache Commons
 * Mutable Object</a> and can be used with anonymous inner classes.
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
        return value.equals( obj );
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
