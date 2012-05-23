package com.turbospaces.api;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.esotericsoftware.kryo.serialize.FieldSerializer;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.MoreExecutors;
import com.turbospaces.collections.GuavaOffHeapCache;
import com.turbospaces.collections.OffHeapLinearProbingSet;
import com.turbospaces.serialization.DecoratedKryo;

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
    private SpaceExpirationListener expirationListener;

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
     */
    public void expireAfterWrite(final int duration,
                                 final TimeUnit unit) {
        ttl = Ints.checkedCast( unit.toMillis( duration ) );
    }

    /**
     * Specify the expiration listener(callback function) that will be trigger once entry expiration detection. This
     * notification can be handled in the same thread, or you may prefer to execute the notification event in
     * asynchronous manner(it is just a matter of passing {@link ExecutorService} implementation and register it with
     * {@link #executorService(ExecutorService) method}).</p>
     * 
     * @param expirationListener
     *            the user callback function
     * 
     * @see MoreExecutors#sameThreadExecutor()
     */
    public void expirationListener(final SpaceExpirationListener expirationListener) {
        this.expirationListener = expirationListener;
    }

    /**
     * Specify the asynchronous expiration notified thread pool. please refer to
     * {@link #expirationListener(SpaceExpirationListener)} method for more details.</p>
     * 
     * By default the {@link MoreExecutors#sameThreadExecutor()} is used.</p>
     * 
     * @param executorService
     *            asynchronous expiration task handler thread pool
     */
    public void executorService(final ExecutorService executorService) {
        this.executorService = executorService;
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
        OffHeapLinearProbingSet offheapSet = new OffHeapLinearProbingSet( capacityRestriction, null, executorService );
        offheapSet.setExpirationListener( expirationListener );
        return new GuavaOffHeapCache<K, V>( offheapSet, kryo, ttl );
    }
}
