package com.turbospaces.collections;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.springframework.data.mapping.model.MutablePersistentEntity;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.collections.OffHeapLinearProbingSegment.ExpiredEntry;
import com.turbospaces.offmemory.ByteArrayPointer;
import com.turbospaces.serialization.PropertiesSerializer;
import com.turbospaces.spaces.CacheStoreEntryWrapper;

/**
 * default implementation of off-heap hash set build on top of multiple concurrent segments with good concurrency for
 * both matchById, matchByTemplate and write operations. </p>
 * 
 * @since 0.1
 * @see OffHeapLinearProbingSegment
 */
public final class OffHeapLinearProbingSet implements OffHeapHashSet {
    private static final float UPPER_LOAD_FACTOR = 0.75f;
    private static final float DOWN_LOAD_FACTOR = 0.15f;
    private static final int MIN_CAPACITY = 1 << 4;

    private final int capacity = MIN_CAPACITY;
    private final AtomicInteger itemsCount = new AtomicInteger();
    private final PropertiesSerializer serializer;
    private final Map<Integer, OffHeapLinearProbingSegment> segments;
    private final MutablePersistentEntity<?, ?> mutablePersistentEntity;
    private final SpaceConfiguration configuration;
    private final ReadWriteLock rehashLock;

    @SuppressWarnings("javadoc")
    public OffHeapLinearProbingSet(final SpaceConfiguration configuration, final MutablePersistentEntity<?, ?> mutablePersistentEntity) {
        this.configuration = configuration;
        this.mutablePersistentEntity = mutablePersistentEntity;
        this.segments = Maps.newHashMap();
        this.rehashLock = new ReentrantReadWriteLock();
        this.serializer = (PropertiesSerializer) configuration.getKryo().getRegisteredClass( mutablePersistentEntity.getType() ).getSerializer();

        // initialize all segments without any delay
        for ( int i = 0; i < capacity; i++ )
            segments.put( Integer.valueOf( i ), new OffHeapLinearProbingSegment( DEFAULT_INITIAL_CAPACITY, configuration, mutablePersistentEntity ) );
    }

    @Override
    public void afterPropertiesSet() {}

    @Override
    public void destroy() {
        for ( OffHeapLinearProbingSegment segment : segments.values() )
            segment.destroy();
    }

    @Override
    public boolean contains(final Object key) {
        final Lock lock = rehashLock.readLock();
        lock.lock();
        try {
            return segments.get( segmentKey( key, capacity ) ).contains( key );
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public ByteArrayPointer getAsPointer(final Object key) {
        final Lock lock = rehashLock.readLock();
        lock.lock();
        try {
            return segments.get( segmentKey( key, capacity ) ).getAsPointer( key );
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public ByteBuffer getAsSerializedData(final Object key) {
        final Lock lock = rehashLock.readLock();
        lock.lock();
        try {
            return segments.get( segmentKey( key, capacity ) ).getAsSerializedData( key );
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public List<ByteArrayPointer> match(final CacheStoreEntryWrapper template) {
        List<ByteArrayPointer> retval = null;
        final Lock lock = rehashLock.readLock();
        lock.lock();
        try {
            for ( OffHeapLinearProbingSegment segment : segments.values() ) {
                List<ByteArrayPointer> match = segment.match( template );
                if ( match != null ) {
                    if ( retval == null )
                        retval = Lists.newLinkedList();
                    retval.addAll( match );
                }
            }
        }
        finally {
            lock.unlock();
        }
        return retval;
    }

    @Override
    public int put(final Object key,
                   final ByteArrayPointer value) {
        int bytes = 0;
        boolean needsRehash = false;
        Lock lock = rehashLock.readLock();
        lock.lock();
        try {
            bytes = segments.get( segmentKey( key, capacity ) ).put( key, value );
            if ( bytes > 0 )
                needsRehash = needsUpperReHash();
        }
        finally {
            lock.unlock();
        }

        // if ( needsRehash )
        // rehash( true );

        return bytes;
    }

    @Override
    public int remove(final Object key) {
        int bytes = 0;
        boolean needsRehash = false;
        Lock lock = rehashLock.readLock();
        lock.lock();
        try {
            bytes = segments.get( segmentKey( key, capacity ) ).remove( key );

            if ( bytes > 0 ) {
                double curload = currenLoad();
                if ( curload >= DOWN_LOAD_FACTOR && ( segments.size() > MIN_CAPACITY ) )
                    needsRehash = true;
            }
        }
        finally {
            lock.unlock();
        }

        // if ( needsRehash )
        // rehash( true );

        return bytes;
    }

    private void rehash(final boolean upper) {
        final Lock lock = rehashLock.writeLock();
        if ( lock.tryLock() )
            try {
                // re-check whether we really need re-hash first under write lock
                if ( ( upper && needsUpperReHash() ) || ( !upper && needsDownRehash() ) ) {
                    Map<Integer, OffHeapLinearProbingSegment> newSegments = Maps.newHashMap();
                    int newCapacity = upper ? capacity << 1 : capacity >>> 1;

                    for ( int i = 0; i < newCapacity; i++ )
                        newSegments.put( Integer.valueOf( i ), new OffHeapLinearProbingSegment(
                                DEFAULT_INITIAL_CAPACITY,
                                configuration,
                                mutablePersistentEntity ) );

                    for ( OffHeapLinearProbingSegment segment : segments.values() )
                        for ( long address : segment.getAddresses() )
                            if ( address != 0 ) {
                                final byte[] serializedData = ByteArrayPointer.getEntityState( address );
                                final ByteBuffer buffer = ByteBuffer.wrap( serializedData );

                                // skip expired
                                if ( ByteArrayPointer.isExpired( address ) ) {
                                    segment.notifyExpired( new ExpiredEntry( buffer, ByteArrayPointer.getTimeToLive( address ) ) );
                                    continue;
                                }

                                Object entryId = serializer.readID( buffer );
                                Integer segmentKey = segmentKey( entryId, newCapacity );
                                OffHeapLinearProbingSegment newSegment = newSegments.get( segmentKey );
                                newSegment.put( entryId, new ByteArrayPointer( address, buffer ) );
                            }
                }
            }
            finally {
                lock.unlock();
            }
    }

    private static Integer segmentKey(final Object key,
                                      final int cap) {
        return Integer.valueOf( ( key.hashCode() & Integer.MAX_VALUE ) % cap );
    }

    /**
     * map probably stale and needs re-hash due to huge removals from set. this is very important for heap memory
     * occupation reduction.
     * 
     * @return whether needs upper re-hash
     */
    private boolean needsDownRehash() {
        return currenLoad() >= DOWN_LOAD_FACTOR && ( segments.size() > MIN_CAPACITY );
    }

    /**
     * map probably stale and needs re-hash due to huge amount of data being recently added to the set. this is very
     * important for good matchByTemplate performance.
     * 
     * @return whether needs down re-hash
     */
    private boolean needsUpperReHash() {
        return currenLoad() >= UPPER_LOAD_FACTOR;
    }

    private double currenLoad() {
        return (double) itemsCount.incrementAndGet() / ( segments.size() * MAX_SEGMENT_CAPACITY );
    }
}
