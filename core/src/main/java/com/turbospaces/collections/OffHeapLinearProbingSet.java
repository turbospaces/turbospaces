package com.turbospaces.collections;

import java.nio.ByteBuffer;
import java.util.List;

import org.springframework.data.mapping.model.MutablePersistentEntity;

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
    private static final int DEFAULT_CONCURRENCY_LEVEL = 128;
    private final OffHeapLinearProbingSegment[] segments;

    private final int concurrency;
    private final int segmentShift;
    private final int segmentMask;

    @SuppressWarnings("javadoc")
    public OffHeapLinearProbingSet(final SpaceConfiguration configuration, final MutablePersistentEntity<?, ?> mutablePersistentEntity) {
        super();
        int sshift = 0;
        int ssize = 1;
        while ( ssize < DEFAULT_CONCURRENCY_LEVEL ) {
            ++sshift;
            ssize <<= 1;
        }
        concurrency = ssize;
        segmentShift = 32 - sshift;
        segmentMask = ssize - 1;
        segments = new OffHeapLinearProbingSegment[concurrency];

        int c = DEFAULT_INITIAL_CAPACITY / ssize;
        if ( c * ssize < DEFAULT_INITIAL_CAPACITY )
            ++c;
        int cap = 1;
        while ( cap < c )
            cap <<= 1;

        for ( int i = 0; i < concurrency; i++ )
            segments[i] = new OffHeapLinearProbingSegment( Math.min( 1 << 4, cap ), configuration, mutablePersistentEntity );
    }

    @Override
    public List<ByteArrayPointer> match(final CacheStoreEntryWrapper template) {
        List<ByteArrayPointer> result = null;
        for ( OffHeapLinearProbingSegment segment : segments )
            if ( segment != null ) {
                List<ByteArrayPointer> segmentMatch = segment.match( template );
                if ( segmentMatch != null ) {
                    if ( result == null )
                        result = Lists.newLinkedList();
                    result.addAll( segmentMatch );
                }
            }
        return result;
    }

    @Override
    public int remove(final Object key) {
        return segmentFor( key ).remove( key );
    }

    @Override
    public int put(final Object key,
                   final ByteArrayPointer value) {
        return segmentFor( key ).put( key, value );
    }

    @Override
    public boolean contains(final Object key) {
        return segmentFor( key ).contains( key );
    }

    @Override
    public ByteArrayPointer getAsPointer(final Object key) {
        return segmentFor( key ).getAsPointer( key );
    }

    @Override
    public ByteBuffer getAsSerializedData(final Object key) {
        return segmentFor( key ).getAsSerializedData( key );
    }

    @Override
    public void afterPropertiesSet() {
        for ( int i = 0; i < concurrency; i++ )
            segments[i].afterPropertiesSet();
    }

    @Override
    public void destroy() {
        for ( int i = 0; i < concurrency; i++ )
            segments[i].destroy();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append( "OffHeap LinerMap: segments" ).append( "\n" );
        for ( int i = 0; i < concurrency; i++ ) {
            builder.append( "\t" );
            builder.append( i ).append( "->" ).append( segments[i] );
            builder.append( "\n" );
        }

        return builder.toString();
    }

    private OffHeapLinearProbingSegment segmentFor(final Object key) {
        final int hash = SpaceUtility.jdkHash( key.hashCode() );
        return segments[hash >>> segmentShift & segmentMask];
    }
}
