package com.turbospaces.collections;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import com.esotericsoftware.kryo.ObjectBuffer;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;
import com.google.common.collect.ImmutableMap;
import com.turbospaces.core.JVMUtil;
import com.turbospaces.model.ExplicitCacheEntry;
import com.turbospaces.offmemory.ByteArrayPointer;
import com.turbospaces.pool.ObjectPool;
import com.turbospaces.serialization.DecoratedKryo;

/**
 * This is guava's cache implementation build on top of {@link OffHeapHashSet} with off-heap cache with absolutely same
 * functionality and semantics(and behavior), except this map scale out of java heap space and dramatically reduce the
 * memory needed in java heap for storing pointer(basically for each key-value pair we need only long's 8 bytes heap
 * memory space).
 * 
 * @since 0.1
 * 
 * @param <K>
 *            key type
 * @param <V>
 *            value type
 */
public class GuavaOffHeapCache<K, V> implements Cache<K, V> {
    private OffHeapHashSet offHeapHashSet;
    private final ObjectPool<ObjectBuffer> objectsPool = JVMUtil.newObjectBufferPool();
    private DecoratedKryo kryo;
    private int ttlAfterWrite;

    @Override
    @SuppressWarnings({ "unchecked" })
    public V getIfPresent(final Object key) {
        ByteBuffer dataBuffer = offHeapHashSet.getAsSerializedData( key );
        ObjectBuffer objectBuffer = objectsPool.borrowObject();
        objectBuffer.setKryo( kryo );

        try {
            ExplicitCacheEntry<K, V> explicitCacheEntry = objectBuffer.readObject( dataBuffer.array(), ExplicitCacheEntry.class );
            return explicitCacheEntry.getBean();
        }
        finally {
            objectsPool.returnObject( objectBuffer );
        }
    }

    @Override
    public V get(final K key,
                 final Callable<? extends V> valueLoader)
                                                         throws ExecutionException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ImmutableMap<K, V> getAllPresent(final Iterable<?> keys) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void put(final K key,
                    final V value) {
        ExplicitCacheEntry<K, V> e = new ExplicitCacheEntry( Preconditions.checkNotNull( key ), Preconditions.checkNotNull( value ) );
        ObjectBuffer objectBuffer = objectsPool.borrowObject();
        objectBuffer.setKryo( kryo );
        try {
            ByteArrayPointer pointer = new ByteArrayPointer( objectBuffer.writeObjectData( e ), e, ttlAfterWrite );
            offHeapHashSet.put( key, pointer );
        }
        finally {
            objectsPool.returnObject( objectBuffer );
        }
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> m) {}

    @Override
    public void invalidate(final Object key) {
        // TODO Auto-generated method stub
    }

    @Override
    public void invalidateAll(final Iterable<?> keys) {
        // TODO Auto-generated method stub

    }

    @Override
    public void invalidateAll() {
        // TODO Auto-generated method stub

    }

    @Override
    public long size() {
        return offHeapHashSet.size();
    }

    @Override
    public CacheStats stats() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ConcurrentMap<K, V> asMap() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void cleanUp() {
        // TODO Auto-generated method stub
    }
}
