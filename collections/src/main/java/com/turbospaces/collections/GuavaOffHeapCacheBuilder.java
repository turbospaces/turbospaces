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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.esotericsoftware.kryo.serialize.FieldSerializer;
import com.google.common.cache.AbstractCache;
import com.google.common.cache.AbstractCache.SimpleStatsCounter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.MoreExecutors;
import com.turbospaces.api.CapacityRestriction;
import com.turbospaces.api.SpaceExpirationListener;
import com.turbospaces.core.MutableObject;
import com.turbospaces.model.ExplicitCacheEntry;
import com.turbospaces.serialization.DecoratedKryo;
import com.turbospaces.serialization.ExplicitCacheEntrySerializer;
import com.turbospaces.serialization.MatchingSerializer;

/**
 * Similar to guava's {@link CacheBuilder}, but builds fast off-heap cache storage rather than heap.
 * 
 * @since 0.1
 * 
 * @param <K>
 *            keys type
 * @param <V>
 *            values type
 */
public class GuavaOffHeapCacheBuilder<K, V> {
    private final CapacityRestriction capacityRestriction = new CapacityRestriction();
    private ExecutorService executorService;
    private DecoratedKryo kryo;
    private int ttl = Integer.MAX_VALUE;
    private SpaceExpirationListener<K, V> expirationListener;
    private boolean recordStats;

    /**
     * Specifies that each entry should be automatically removed from the cache once a fixed duration
     * has elapsed after the entry's creation, or the most recent replacement of its value.
     * 
     * <p>
     * Expired entries may be counted in {@link OffHeapLinearProbingSet#size}, but will never be visible to read or
     * write operations. Expired entries are cleaned up as part of the routine maintenance described in the class
     * javadoc.
     * 
     * @param duration
     *            the length of time after an entry is created that it should be automatically
     *            removed
     * @param unit
     *            the unit that {@code duration} is expressed in
     * @return this
     */
    public GuavaOffHeapCacheBuilder<K, V> expireAfterWrite(final int duration,
                                                           final TimeUnit unit) {
        ttl = Ints.checkedCast( ( unit == null ? TimeUnit.MILLISECONDS : unit ).toMillis( duration ) );
        return this;
    }

    /**
     * Specify the expiration listener(callback function) that will be trigger once entry expiration detection. This
     * notification can be handled in the same thread, or you may prefer to execute the notification event in
     * asynchronous manner(it is just a matter of passing {@link ExecutorService} implementation and register it with
     * {@link #executorService(ExecutorService) method}).</p>
     * 
     * @param expirationListener
     *            the user callback function
     * @return this
     * 
     * @see MoreExecutors#sameThreadExecutor()
     */
    public GuavaOffHeapCacheBuilder<K, V> expirationListener(final SpaceExpirationListener<K, V> expirationListener) {
        this.expirationListener = expirationListener;
        return this;
    }

    /**
     * Specify the asynchronous expiration notified thread pool. please refer to
     * {@link #expirationListener(SpaceExpirationListener)} method for more details.</p>
     * 
     * By default the {@link MoreExecutors#sameThreadExecutor()} is used.</p>
     * 
     * @param executorService
     *            asynchronous expiration task handler thread pool
     * @return this
     */
    public GuavaOffHeapCacheBuilder<K, V> executorService(final ExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }

    /**
     * Specify the custom kryo serialization container(fabric) - this is completely optional step and should be
     * carefully used in case you want to get some specific behavior.
     * 
     * @param kryo
     *            custom kryo fabric
     * @return this
     */
    public GuavaOffHeapCacheBuilder<K, V> kryo(final DecoratedKryo kryo) {
        this.kryo = kryo;
        return this;
    }

    /**
     * Enable the accumulation of {@link CacheStats} during the operation of the cache. Without this
     * {@link Cache#stats()} will throw {@link IllegalStateException}. Note that recording statistics requires
     * bookkeeping to be performed with each operation, and thus imposes a performance penalty on
     * cache operation.
     * 
     * @param record
     *            whether record cache statistic
     * 
     * @return this
     */
    public GuavaOffHeapCacheBuilder<K, V> recordStats(final boolean record) {
        this.recordStats = record;
        return this;
    }

    /**
     * Builds new off-heap linear probing set.
     * 
     * <p>
     * This method does not alter the state of this {@code OffHeapSetBuilder} instance, so it can be invoked again to
     * create multiple independent caches.
     * 
     * @param clazz
     *            the class of value's type
     * 
     * @return new off-heap linear probing set having the requested features
     */
    public Cache<K, V> build(final Class<V> clazz) {
        if ( executorService == null )
            executorService = MoreExecutors.sameThreadExecutor();
        if ( kryo == null )
            kryo = new DecoratedKryo();
        kryo.register( clazz, new FieldSerializer( kryo, clazz ) );
        MatchingSerializer<?> serializer = new ExplicitCacheEntrySerializer( kryo );
        final MutableObject<SimpleStatsCounter> statsCounter = new MutableObject<AbstractCache.SimpleStatsCounter>();
        if ( recordStats )
            statsCounter.set( new SimpleStatsCounter() );
        OffHeapLinearProbingSet offheapSet = new OffHeapLinearProbingSet( capacityRestriction, serializer, executorService );
        SpaceExpirationListener<K, ByteBuffer> evictionListener = new SpaceExpirationListener<K, ByteBuffer>( false ) {
            @Override
            public void handleNotification(final ByteBuffer entity,
                                           final K id,
                                           final Class<ByteBuffer> persistentClass,
                                           final int originalTimeToLive) {
                if ( statsCounter.get() != null )
                    statsCounter.get().recordEviction();
            }
        };
        if ( expirationListener != null )
            offheapSet.setExpirationListeners( evictionListener, new SpaceExpirationListener<K, ExplicitCacheEntry<K, V>>() {
                @SuppressWarnings("unchecked")
                @Override
                public void handleNotification(final ExplicitCacheEntry<K, V> entity,
                                               final K id,
                                               final Class<ExplicitCacheEntry<K, V>> persistentClass,
                                               final int originalTimeToLive) {
                    expirationListener.handleNotification( entity.getBean(), id, (Class<V>) entity.getBean().getClass(), originalTimeToLive );
                }
            } );
        else
            offheapSet.setExpirationListeners( evictionListener );
        return new GuavaOffHeapCache<K, V>( offheapSet, kryo, ttl, statsCounter.get() );
    }
}
