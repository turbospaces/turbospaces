package com.turbospaces.collections;

import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.math.IntMath;
import com.turbospaces.api.CapacityRestriction;
import com.turbospaces.api.SpaceExpirationListener;
import com.turbospaces.core.JVMUtil;
import com.turbospaces.model.BO;
import com.turbospaces.model.CacheStoreEntryWrapper;
import com.turbospaces.offmemory.ByteArrayPointer;
import com.turbospaces.serialization.PropertiesSerializer;

/**
 * Default implementation of off-heap hash set build on top of multiple concurrent segments with good concurrency for
 * both <code>matchById, matchByTemplate and write</code> with minimal GC impact.</p>
 * </p>
 * 
 * @since 0.1
 */
public final class OffHeapLinearProbingSet implements OffHeapHashSet {
    private final OffHeapLinearProbingSegment[] segments;

    /**
     * create new off-heap linear set for the given {@link BO} class.
     * 
     * @param capacityRestriction
     *            the capacity restrictor
     * @param serializer
     *            entity serializer
     * @param executorService
     *            concurrent executor service
     */
    public OffHeapLinearProbingSet(final CapacityRestriction capacityRestriction,
                                   final PropertiesSerializer serializer,
                                   final ExecutorService executorService) {
        this( IntMath.pow( 2, IntMath.log2(
                Math.max( (int) ( capacityRestriction.getMaxElements() / OffHeapLinearProbingSegment.MAX_SEGMENT_CAPACITY ), 1 ),
                RoundingMode.UP ) ), serializer, executorService );
    }

    @VisibleForTesting
    OffHeapLinearProbingSet(final int initialSegments, final PropertiesSerializer serializer, final ExecutorService executorService) {
        segments = new OffHeapLinearProbingSegment[initialSegments];
        for ( int i = 0; i < initialSegments; i++ )
            segments[i] = new OffHeapLinearProbingSegment( serializer, executorService );
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
    public void setExpirationListener(final SpaceExpirationListener expirationListener) {
        for ( OffHeapLinearProbingSegment entry : segments )
            entry.setExpirationListener( expirationListener );
    }

    /**
     * similar to the spring's afterPropertySet()
     */
    public void afterPropertiesSet() {
        // TODO: schedule cleanup task
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
        return segments[( JVMUtil.jdkRehash( key.hashCode() ) & Integer.MAX_VALUE ) % segments.length];
    }
}
