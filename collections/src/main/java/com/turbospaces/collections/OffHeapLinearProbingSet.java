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
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.lmax.disruptor.util.Util;
import com.turbospaces.api.CacheEvictionPolicy;
import com.turbospaces.api.CapacityRestriction;
import com.turbospaces.api.SpaceExpirationListener;
import com.turbospaces.core.EffectiveMemoryManager;
import com.turbospaces.core.JVMUtil;
import com.turbospaces.model.BO;
import com.turbospaces.model.CacheStoreEntryWrapper;
import com.turbospaces.offmemory.ByteArrayPointer;
import com.turbospaces.serialization.MatchingSerializer;

/**
 * Default implementation of off-heap hash set build on top of multiple concurrent segments with good concurrency for
 * both <code>matchById, matchByTemplate and write</code> with minimal GC impact.</p>
 * </p>
 * 
 * @since 0.1
 */
public final class OffHeapLinearProbingSet implements OffHeapHashSet {
    private final OffHeapLinearProbingSegment[] segments;
    private final int size, mask;

    /**
     * create new off-heap linear set for the given {@link BO} class.
     * 
     * @param memoryManager
     *            off-heap memory manager
     * @param capacityRestriction
     *            the capacity restrictor
     * @param serializer
     *            entity serializer
     * @param executorService
     *            concurrent executor service
     */
    public OffHeapLinearProbingSet(final EffectiveMemoryManager memoryManager,
                                   final CapacityRestriction capacityRestriction,
                                   final MatchingSerializer<?> serializer,
                                   final ExecutorService executorService) {
        this( memoryManager, Util.ceilingNextPowerOfTwo( Math.max(
                (int) ( capacityRestriction.getMaxElements() / OffHeapLinearProbingSegment.MAX_SEGMENT_CAPACITY ),
                1 ) ), serializer, executorService, capacityRestriction.getEvictionPolicy() );
    }

    @VisibleForTesting
    OffHeapLinearProbingSet(final EffectiveMemoryManager memoryManager,
                            final int initialSegments,
                            final MatchingSerializer<?> serializer,
                            final ExecutorService executorService,
                            final CacheEvictionPolicy cacheEvictionPolicy) {
        this.segments = new OffHeapLinearProbingSegment[initialSegments];
        for ( int i = 0; i < initialSegments; i++ )
            segments[i] = new OffHeapLinearProbingSegment( memoryManager, serializer, executorService, cacheEvictionPolicy );
        this.size = segments.length;
        this.mask = this.size - 1;
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

    @SuppressWarnings("rawtypes")
    @Override
    public void setExpirationListeners(final SpaceExpirationListener... expirationListeners) {
        for ( OffHeapLinearProbingSegment entry : segments )
            entry.setExpirationListeners( expirationListeners );
    }

    @Override
    public int size() {
        int size = 0;
        for ( OffHeapLinearProbingSegment entry : segments )
            size += entry.size();
        return size;
    }

    @Override
    public void cleanUp() {
        for ( OffHeapLinearProbingSegment entry : segments )
            entry.cleanUp();
    }

    @Override
    public int evictPercentage(final int percentage) {
        int evictedObjects = 0;
        for ( OffHeapLinearProbingSegment entry : segments )
            evictedObjects += entry.evictPercentage( percentage );
        return evictedObjects;
    }

    @Override
    public int evictElements(final int elements) {
        int evictedObjects = 0;
        for ( OffHeapLinearProbingSegment entry : segments )
            evictedObjects += entry.evictElements( elements );
        return evictedObjects;
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
        return segments[JVMUtil.murmurRehash( key.hashCode() ) & Integer.MAX_VALUE & mask];
    }
}
