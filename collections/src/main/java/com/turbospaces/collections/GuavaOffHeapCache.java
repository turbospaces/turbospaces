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
package com.turbospaces.collections;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.esotericsoftware.kryo.ObjectBuffer;
import com.esotericsoftware.minlog.Log;
import com.google.common.base.Preconditions;
import com.google.common.cache.AbstractCache;
import com.google.common.cache.CacheStats;
import com.turbospaces.core.EffectiveMemoryManager;
import com.turbospaces.model.ExplicitCacheEntry;
import com.turbospaces.offmemory.ByteArrayPointer;
import com.turbospaces.serialization.DecoratedKryo;

/**
 * This is guava's cache implementation build on top of {@link OffHeapHashSet} with off-heap cache with absolutely same
 * functionality and semantics(and behavior), except this map scale out of java heap space and dramatically reduce the
 * memory needed in java heap for storing pointer(basically for each key-value pair we need only long's 8 bytes heap
 * memory space).</p>
 * 
 * <b> NOTE: </b> - it's not recommended to you this class directly and you would prefer
 * {@link GuavaOffHeapCacheBuilder} instead.
 * 
 * @since 0.1
 * 
 * @param <K>
 *            key type
 * @param <V>
 *            value type
 */
public final class GuavaOffHeapCache<K, V> extends AbstractCache<K, V> implements EvictableCache {
    private final EffectiveMemoryManager memoryManager;
    private final OffHeapHashSet offHeapHashSet;
    private final DecoratedKryo kryo;
    private final int ttlAfterWrite;
    private final SimpleStatsCounter statsCounter;

    /**
     * create new guava's cache over off-heap set delegate and associated kryo serializer. also time-2-live must be
     * explicitly passed.
     * 
     * @param memoryManager
     *            off-heap memory manager
     * @param offHeapHashSet
     *            off-heap cache collection
     * @param kryo
     *            serialization provider
     * @param ttlAfterWrite
     *            time-to-live after write
     * @param statsCounter
     *            statistics counter
     */
    public GuavaOffHeapCache(final EffectiveMemoryManager memoryManager,
                             final OffHeapHashSet offHeapHashSet,
                             final DecoratedKryo kryo,
                             final int ttlAfterWrite,
                             final SimpleStatsCounter statsCounter) {
        this.memoryManager = memoryManager;
        this.offHeapHashSet = offHeapHashSet;
        this.kryo = kryo;
        this.ttlAfterWrite = ttlAfterWrite;
        this.statsCounter = statsCounter;
    }

    @Override
    public V getIfPresent(final Object key) {
        return getIfPresent( key, true );
    }

    @Override
    public V get(final K key,
                 final Callable<? extends V> valueLoader)
                                                         throws ExecutionException {
        V value = getIfPresent( key, true );
        if ( value == null )
            synchronized ( valueLoader ) {
                long nano = 0;
                try {
                    // re-check under lock first and do not increment cache misses as this is internal barrier
                    value = getIfPresent( key, false );
                    if ( value == null ) {
                        if ( statsCounter != null )
                            nano = System.nanoTime();
                        value = valueLoader.call();
                        if ( nano != 0 )
                            statsCounter.recordLoadSuccess( System.nanoTime() - nano );
                        // and finally add loaded value
                        put( key, Preconditions.checkNotNull( value, "Cache loader didn't load any value" ) );
                    }
                }
                catch ( Exception e ) {
                    Log.error( e.getMessage(), e );
                    // record exception
                    if ( nano != 0 )
                        statsCounter.recordLoadException( System.nanoTime() - nano );
                    throw new ExecutionException( e );
                }
            }
        return value;
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void put(final K key,
                    final V value) {
        ExplicitCacheEntry<K, V> e = new ExplicitCacheEntry( Preconditions.checkNotNull( key ), Preconditions.checkNotNull( value ) );
        ObjectBuffer objectBuffer = new ObjectBuffer( kryo );
        ByteArrayPointer pointer = new ByteArrayPointer( memoryManager, objectBuffer.writeObjectData( e ), e, ttlAfterWrite );
        offHeapHashSet.put( key, pointer );
    }

    @Override
    public void invalidate(final Object key) {
        offHeapHashSet.remove( key );
    }

    @Override
    public long size() {
        return offHeapHashSet.size();
    }

    @Override
    public CacheStats stats() {
        Preconditions.checkState( statsCounter != null, "statistics disabled for this class" );
        return statsCounter.snapshot();
    }

    @Override
    public void cleanUp() {
        offHeapHashSet.cleanUp();
    }

    @Override
    public void invalidateAll() {
        offHeapHashSet.destroy();
    }

    @Override
    public int evictPercentage(final int percentage) {
        return offHeapHashSet.evictPercentage( percentage );
    }

    @Override
    public int evictElements(final int elements) {
        return offHeapHashSet.evictElements( elements );
    }

    @Override
    public String toString() {
        return offHeapHashSet.toString();
    }

    @SuppressWarnings("unchecked")
    private V getIfPresent(final Object key,
                           final boolean recordStatistics) {
        ByteBuffer dataBuffer = offHeapHashSet.getAsSerializedData( key );
        V result = null;

        if ( dataBuffer != null ) {
            ObjectBuffer objectBuffer = new ObjectBuffer( kryo );

            ExplicitCacheEntry<K, V> explicitCacheEntry = objectBuffer.readObjectData( dataBuffer.array(), ExplicitCacheEntry.class );
            result = explicitCacheEntry.getBean();
            if ( recordStatistics && statsCounter != null )
                statsCounter.recordHits( 1 );
        }
        else if ( recordStatistics && statsCounter != null )
            statsCounter.recordMisses( 1 );
        return result;
    }
}
