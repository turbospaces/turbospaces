package com.turbospaces.collections;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.mapping.model.MutablePersistentEntity;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.turbospaces.api.AbstractSpaceConfiguration;
import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.core.SpaceUtility;
import com.turbospaces.offmemory.ByteArrayPointer;
import com.turbospaces.serialization.EntitySerializer;
import com.turbospaces.serialization.PropertiesSerializer;
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
    private static final Logger LOGGER = LoggerFactory.getLogger( OffHeapLinearProbingSet.class );
    static final float UPPER_LOAD_FACTOR = 0.75f;
    static final float DOWN_LOAD_FACTOR = 0.15f;
    static final int MIN_SEGMENTS = 1 << 12;
    static final int MAX_SEGMENTS = 1 << 16;

    private final AtomicInteger itemsCount = new AtomicInteger();
    private final PropertiesSerializer serializer;
    private final MutablePersistentEntity<?, ?> mutablePersistentEntity;
    private final SpaceConfiguration configuration;
    private final OffHeapLinearProbingSegment[] segments;

    @SuppressWarnings("javadoc")
    public OffHeapLinearProbingSet(final SpaceConfiguration configuration, final MutablePersistentEntity<?, ?> mutablePersistentEntity) {
        this.configuration = configuration;
        this.mutablePersistentEntity = mutablePersistentEntity;
        this.serializer = (PropertiesSerializer) configuration.getKryo().getRegisteredClass( mutablePersistentEntity.getType() ).getSerializer();
        this.segments = new OffHeapLinearProbingSegment[MAX_SEGMENTS];

        addSegments( MIN_SEGMENTS );
    }

    @Override
    public void afterPropertiesSet() {}

    @Override
    public boolean contains(final Object key) {
        return findFirstSegment( key ).contains( key );
    }

    @Override
    public ByteArrayPointer getAsPointer(final Object key) {
        return findFirstSegment( key ).getAsPointer( key );
    }

    @Override
    public ByteBuffer getAsSerializedData(final Object key) {
        return findFirstSegment( key ).getAsSerializedData( key );
    }

    @Override
    public List<ByteArrayPointer> match(final CacheStoreEntryWrapper template) {
        List<ByteArrayPointer> retval = null;
        for ( OffHeapLinearProbingSegment entry : segments )
            if ( entry != null ) {
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
        int bytes = 0;
        boolean needsRehash = false;
        bytes = findFirstSegment( key ).put( key, value );
        if ( bytes == 0 ) {
            itemsCount.incrementAndGet();
            needsRehash = needsUpperReHash();
        }
        if ( needsRehash ) {
            // TODO: re-hash
        }
        return bytes;
    }

    @Override
    public int remove(final Object key) {
        int bytes = 0;
        boolean needsRehash = false;
        bytes = findFirstSegment( key ).remove( key );
        if ( bytes == 0 ) {
            itemsCount.decrementAndGet();
            needsRehash = needsDownRehash();
        }
        if ( needsRehash ) {
            // TODO: re-hash
        }
        return bytes;
    }

    @Override
    public void destroy() {
        for ( OffHeapLinearProbingSegment entry : segments )
            if ( entry != null )
                entry.destroy();
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder( Objects.toStringHelper( this ).toString() ).append( "totalItems=" + itemsCount );
        builder.append( "\n" );
        int i = 0;
        for ( OffHeapLinearProbingSegment entry : segments )
            if ( entry != null ) {
                builder.append( "\t" );
                builder.append( i++ ).append( "->" ).append( entry );
                builder.append( "\n" );
            }
        return builder.toString();
    }

    /**
     * add new segments dynamically
     * 
     * @param segmentsCount
     *            how many segments to add
     */
    private void addSegments(final int segmentsCount) {
        for ( int j = 0; j < segmentsCount; j++ ) {
            OffHeapLinearProbingSegment segment = new OffHeapLinearProbingSegment( DEFAULT_INITIAL_CAPACITY, configuration, mutablePersistentEntity );
            int hash = ( System.identityHashCode( segment ) & Integer.MAX_VALUE ) % MAX_SEGMENTS;
            for ( int i = hash; i < hash + MAX_SEGMENTS; i++ ) {
                int idx = ( i % MAX_SEGMENTS );
                if ( segments[idx] == null ) {
                    segments[idx] = segment;
                    break;
                }
            }
        }
        LOGGER.trace( "added {} segments, segments = {}", segmentsCount, segments );
    }

    /**
     * find the very first segment for the given key using consistent hash function.
     * 
     * @param key
     *            object's id
     * @return segment for key
     */
    private OffHeapLinearProbingSegment findFirstSegment(final Object key) {
        final int index = ( SpaceUtility.jdkHash( key.hashCode() ) & Integer.MAX_VALUE ) % MAX_SEGMENTS;

        for ( int i = index; i < index + MAX_SEGMENTS; i++ ) {
            final int idx = ( i % MAX_SEGMENTS );
            final OffHeapLinearProbingSegment retval = segments[idx];
            if ( retval != null )
                return retval;
        }
        throw new IllegalStateException();
    }

    /**
     * map probably stale and needs re-hash due to huge removals from set. this is very important for heap memory
     * occupation reduction.
     * 
     * @return whether needs upper re-hash
     */
    @VisibleForTesting
    boolean needsDownRehash() {
        int threshold = (int) ( segments.length * MAX_SEGMENT_CAPACITY * DOWN_LOAD_FACTOR );
        return itemsCount.get() <= threshold && ( segments.length > MIN_SEGMENTS );
    }

    /**
     * map probably stale and needs re-hash due to huge amount of data being recently added to the set. this is very
     * important for good matchByTemplate performance.
     * 
     * @return whether needs down re-hash
     */
    @VisibleForTesting
    boolean needsUpperReHash() {
        int threshold = (int) ( segments.length * MAX_SEGMENT_CAPACITY * UPPER_LOAD_FACTOR );
        return itemsCount.get() >= threshold;
    }
}
