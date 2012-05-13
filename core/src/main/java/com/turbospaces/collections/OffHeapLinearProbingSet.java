package com.turbospaces.collections;

import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.List;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.mapping.model.MutablePersistentEntity;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.math.IntMath;
import com.turbospaces.api.AbstractSpaceConfiguration;
import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.core.SpaceUtility;
import com.turbospaces.model.BO;
import com.turbospaces.offmemory.ByteArrayPointer;
import com.turbospaces.serialization.EntitySerializer;
import com.turbospaces.spaces.CacheStoreEntryWrapper;

/**
 * Default implementation of off-heap hash set build on top of multiple concurrent segments with good concurrency for
 * both <code>matchById, matchByTemplate and write</code> operations build on top of <b>consistent hash
 * algorithm</b>(making re-hashing and re-balancing of internal segments much easier and faster and much better for GC).
 * </p>
 * 
 * @since 0.1
 * 
 * @see OffHeapLinearProbingSegment
 * @see AbstractSpaceConfiguration
 * @see MutablePersistentEntity
 * @see EntitySerializer
 * @see OffHeapLinearProbingSegment.ExpiredEntry
 * @see org.jgroups.blocks.PartitionedHashMap.ConsistentHashFunction
 */
public final class OffHeapLinearProbingSet implements OffHeapHashSet, InitializingBean {
    private final OffHeapLinearProbingSegment[] segments;

    /**
     * create new off-heap linear set for the give {@link BO} class.
     * 
     * @param configuration
     *            space configuration
     * @param bo
     *            particular business class(space registered class)
     */
    @SuppressWarnings("rawtypes")
    public OffHeapLinearProbingSet(final SpaceConfiguration configuration, final BO bo) {
        this( IntMath.pow( 2, IntMath.log2(
                (int) ( bo.getCapacityRestriction().getMaxElements() / OffHeapLinearProbingSegment.MAX_SEGMENT_CAPACITY ),
                RoundingMode.UP ) ), bo, configuration );
    }

    @VisibleForTesting
    OffHeapLinearProbingSet(final int initialSegments, final MutablePersistentEntity<?, ?> bo, final SpaceConfiguration configuration) {
        segments = new OffHeapLinearProbingSegment[initialSegments];
        for ( int i = 0; i < initialSegments; i++ )
            segments[i] = new OffHeapLinearProbingSegment( configuration, bo );
    }

    @Override
    public void afterPropertiesSet() {}

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
    public List<ByteArrayPointer> match(final CacheStoreEntryWrapper template) {
        List<ByteArrayPointer> retval = null;
        for ( OffHeapLinearProbingSegment entry : segments ) {
            List<ByteArrayPointer> match = entry.match( template );
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
        return segmentFor( key ).put( key, value );
    }

    @Override
    public int remove(final Object key) {
        return segmentFor( key ).remove( key );
    }

    @Override
    public void destroy() {
        for ( OffHeapLinearProbingSegment entry : segments )
            entry.destroy();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder( Objects.toStringHelper( this ).toString() );
        builder.append( "\n" );
        int i = 0, totalItemsCount = 0;
        for ( OffHeapLinearProbingSegment entry : segments ) {
            builder.append( "\t" );
            builder.append( i++ ).append( "->" ).append( entry );
            builder.append( "\n" );
            totalItemsCount += entry.size();
        }
        builder.append( "\n" ).append( "totalItems=" + totalItemsCount );
        return builder.toString();
    }

    private OffHeapLinearProbingSegment segmentFor(final Object key) {
        return segments[( SpaceUtility.jdkRehash( key.hashCode() ) & Integer.MAX_VALUE ) % segments.length];
    }
}
