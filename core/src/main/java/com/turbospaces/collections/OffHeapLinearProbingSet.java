package com.turbospaces.collections;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.data.mapping.model.MutablePersistentEntity;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.core.SpaceUtility;
import com.turbospaces.offmemory.ByteArrayPointer;
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

    private final AtomicInteger itemsCount = new AtomicInteger();
    private final int capacity = MIN_CAPACITY;

    private final Function<Integer, OffHeapLinearProbingSegment> compFunc;
    private final ConcurrentMap<Integer, OffHeapLinearProbingSegment> segments;

    @SuppressWarnings("javadoc")
    public OffHeapLinearProbingSet(final SpaceConfiguration configuration, final MutablePersistentEntity<?, ?> mutablePersistentEntity) {
        compFunc = new Function<Integer, OffHeapLinearProbingSegment>() {
            @Override
            public OffHeapLinearProbingSegment apply(final Integer input) {
                return new OffHeapLinearProbingSegment( capacity, configuration, mutablePersistentEntity );
            }
        };
        this.segments = SpaceUtility.newCompMap( compFunc );
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
        return segments.get( segmentKey( key ) ).contains( key );
    }

    @Override
    public ByteArrayPointer getAsPointer(final Object key) {
        return segments.get( segmentKey( key ) ).getAsPointer( key );
    }

    @Override
    public ByteBuffer getAsSerializedData(final Object key) {
        return segments.get( segmentKey( key ) ).getAsSerializedData( key );
    }

    @Override
    public List<ByteArrayPointer> match(final CacheStoreEntryWrapper template) {
        List<ByteArrayPointer> retval = null;
        for ( OffHeapLinearProbingSegment segment : segments.values() ) {
            List<ByteArrayPointer> match = segment.match( template );
            if ( match != null ) {
                if ( retval == null )
                    retval = Lists.newLinkedList();
                retval.addAll( match );
            }
        }
        return retval;
    }

    @Override
    public int put(final Object key,
                   final ByteArrayPointer value) {
        Integer segmentKey = segmentKey( key );
        OffHeapLinearProbingSegment segment = segments.get( segmentKey );

        int bytes = segment.put( key, value );
        if ( bytes > 0 ) {
            int curval = itemsCount.incrementAndGet();
            double curload = curval / ( segments.size() * MAX_SEGMENT_CAPACITY );
            if ( curload >= UPPER_LOAD_FACTOR ) {
                // TODO: dynamically re-hash table
            }
        }
        return bytes;
    }

    @Override
    public int remove(final Object key) {
        int bytes = segments.get( segmentKey( key ) ).remove( key );
        if ( bytes > 0 ) {
            int curval = itemsCount.decrementAndGet();
            double curload = curval / ( segments.size() * MAX_SEGMENT_CAPACITY );
            if ( curload >= DOWN_LOAD_FACTOR && ( segments.size() > MIN_CAPACITY ) ) {
                // TODO: dynamically re-hash table
            }
        }
        return bytes;
    }

    private Integer segmentKey(final Object key) {
        return Integer.valueOf( ( key.hashCode() & Integer.MAX_VALUE ) % capacity );
    }
}
