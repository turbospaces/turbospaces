package com.turbospaces.collections;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.springframework.data.mapping.model.MutablePersistentEntity;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.core.ConsistentHasher;
import com.turbospaces.core.MutableObject;
import com.turbospaces.offmemory.ByteArrayPointer;
import com.turbospaces.spaces.CacheStoreEntryWrapper;

/**
 * default implementation of off-heap hash set build on top of multiple concurrent segments with good concurrency for
 * both matchById, matchByTemplate and write operations. </p>
 * 
 * @since 0.1
 * @see OffHeapLinearProbingSegment
 * @see ConsistentHasher
 */
public final class OffHeapLinearProbingSet implements OffHeapHashSet {
    static final int MAX_SEGMENTS_COUNT = ( 1 << 8 ) * ( 1 << 10 );
    private final ConsistentHasher<OffHeapLinearProbingSegment> consistentHasher;

    @SuppressWarnings("javadoc")
    public OffHeapLinearProbingSet(final SpaceConfiguration configuration, final MutablePersistentEntity<?, ?> mutablePersistentEntity) {
        int initialSegments = Math.abs( Byte.MIN_VALUE );
        int maxSegments = initialSegments;

        consistentHasher = new ConsistentHasher<OffHeapLinearProbingSegment>(
                initialSegments,
                maxSegments,
                new Supplier<OffHeapLinearProbingSegment>() {

                    @Override
                    public OffHeapLinearProbingSegment get() {
                        return new OffHeapLinearProbingSegment( DEFAULT_INITIAL_CAPACITY, configuration, mutablePersistentEntity );
                    }
                } );
    }

    @Override
    public List<ByteArrayPointer> match(final CacheStoreEntryWrapper template) {
        final MutableObject<List<ByteArrayPointer>> retval = new MutableObject<List<ByteArrayPointer>>();
        consistentHasher.forEachSegment( new Function<OffHeapLinearProbingSegment, List<ByteArrayPointer>>() {
            @Override
            public List<ByteArrayPointer> apply(final OffHeapLinearProbingSegment input) {
                List<ByteArrayPointer> match = input.match( template );
                if ( match != null ) {
                    if ( retval.get() == null )
                        retval.set( new LinkedList<ByteArrayPointer>() );
                    retval.get().addAll( match );
                }
                return match;
            }
        } );
        return retval.get();
    }

    @Override
    public int remove(final Object key) {
        return consistentHasher.segmentFor( key ).remove( key );
    }

    @Override
    public int put(final Object key,
                   final ByteArrayPointer value) {
        return consistentHasher.segmentFor( key ).put( key, value );
    }

    @Override
    public boolean contains(final Object key) {
        return consistentHasher.segmentFor( key ).contains( key );
    }

    @Override
    public ByteArrayPointer getAsPointer(final Object key) {
        return consistentHasher.segmentFor( key ).getAsPointer( key );
    }

    @Override
    public ByteBuffer getAsSerializedData(final Object key) {
        return consistentHasher.segmentFor( key ).getAsSerializedData( key );
    }

    @Override
    public void afterPropertiesSet() {
        consistentHasher.forEachSegment( new Function<OffHeapLinearProbingSegment, Void>() {
            @Override
            public Void apply(final OffHeapLinearProbingSegment input) {
                input.afterPropertiesSet();
                return null;
            }
        } );
    }

    @Override
    public void destroy() {
        consistentHasher.forEachSegment( new Function<OffHeapLinearProbingSegment, Void>() {
            @Override
            public Void apply(final OffHeapLinearProbingSegment input) {
                input.destroy();
                return null;
            }
        } );
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append( "OffHeap LinearMap: segments" ).append( "\n" );

        consistentHasher.forEachSegment( new Function<OffHeapLinearProbingSegment, Void>() {
            private int i = 0;

            @Override
            public Void apply(final OffHeapLinearProbingSegment input) {
                builder.append( "\t" );
                builder.append( i++ ).append( "->" ).append( input );
                builder.append( "\n" );

                return null;
            }
        } );

        return builder.toString();
    }
}
