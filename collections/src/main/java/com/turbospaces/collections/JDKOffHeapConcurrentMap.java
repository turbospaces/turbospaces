package com.turbospaces.collections;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * @param <K>
 *            key's type
 * @param <V>
 *            value's type
 * @since 0.1
 */
public final class JDKOffHeapConcurrentMap<K, V> implements ConcurrentMap<K, V> {
    private final OffHeapHashSet offHashSet;

    private JDKOffHeapConcurrentMap(final OffHeapHashSet offHashSet) {
        super();
        this.offHashSet = offHashSet;
    }

    @Override
    public int size() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isEmpty() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean containsKey(final Object key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean containsValue(final Object value) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public V get(final Object key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public V put(final K key,
                 final V value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public V remove(final Object key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> m) {
        // TODO Auto-generated method stub

    }

    @Override
    public void clear() {
        // TODO Auto-generated method stub

    }

    @Override
    public Set<K> keySet() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Collection<V> values() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public V putIfAbsent(final K key,
                         final V value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean remove(final Object key,
                          final Object value) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean replace(final K key,
                           final V oldValue,
                           final V newValue) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public V replace(final K key,
                     final V value) {
        // TODO Auto-generated method stub
        return null;
    }
}
