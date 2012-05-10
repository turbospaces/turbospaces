package com.turbospaces.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;

/**
 * This is consistent hasher utility class. Designed for both dynamic nodes add and removal. Good thing about this class
 * is that it is generic one and can used for different types(network address for example).</p>
 * 
 * @param <S>
 *            type of cache segments(slots)
 * @since 0.1
 * @see org.jgroups.blocks.PartitionedHashMap.ConsistentHashFunction
 */
@ThreadSafe
public final class ConsistentHasher<S> {
    private final Logger logger = LoggerFactory.getLogger( getClass() );

    private final SortedMap<Integer, S> segments = Maps.newTreeMap();
    private final ReadWriteLock rwLock;
    private final int maxSegmentsCount;
    private final Supplier<S> segmentInstantiationFunction;

    /**
     * create consistent hasher with given {@code maxSegmentsCount} (maximum segments count to which this consistent
     * hasher can grow) and segment instantiation callback.
     * 
     * @param maxSegmentsCount
     * @param initialSegmentsCount
     * @param segmentInstantiationFunction
     */
    public ConsistentHasher(final int initialSegmentsCount, final int maxSegmentsCount, final Supplier<S> segmentInstantiationFunction) {
        this.maxSegmentsCount = maxSegmentsCount;
        this.segmentInstantiationFunction = segmentInstantiationFunction;
        this.rwLock = new ReentrantReadWriteLock();

        List<S> newSegments = new ArrayList<S>( initialSegmentsCount );
        for ( int i = 0; i < initialSegmentsCount; i++ ) {
            S s = this.segmentInstantiationFunction.get();
            newSegments.add( s );
        }
        addSegments( newSegments );
        logger.debug( "maxSegments={}, initialSegments={}, segments={}", new Object[] { maxSegmentsCount, initialSegmentsCount, segments } );
    }

    /**
     * dynamically add new segment nodes.
     * 
     * @param newSegments
     */
    public void addSegments(final List<S> newSegments) {
        final Lock writeLock = rwLock.writeLock();

        writeLock.lock();
        try {
            for ( S segment : newSegments ) {
                int hash = ( System.identityHashCode( segment ) & Integer.MAX_VALUE ) % maxSegmentsCount;
                for ( int i = hash; i < hash + maxSegmentsCount; i++ ) {
                    int idx = ( i % maxSegmentsCount );
                    if ( !segments.containsKey( idx ) ) {
                        segments.put( Integer.valueOf( idx ), segment );
                        break;
                    }
                }
            }
        }
        finally {
            writeLock.unlock();
        }
    }

    /**
     * consistently calculate the proper segment for key using consistent hash algorithm.
     * 
     * @param key
     * @return proper segment(slot) for hash
     */
    public S segmentFor(final Object key) {
        final int hash = key.hashCode() & Integer.MAX_VALUE;
        final Lock readLock = rwLock.readLock();

        readLock.lock();
        try {
            int segmentIndex = hash % maxSegmentsCount;
            return findFirst( segmentIndex );
        }
        finally {
            readLock.unlock();
        }
    }

    /**
     * apply user function to the each segment.
     * 
     * @param f
     *            user function
     */
    public <V> void forEachSegment(final Function<S, V> f) {
        final Lock readLock = rwLock.readLock();
        Collection<S> values = null;

        readLock.lock();
        try {
            values = segments.values();
        }
        finally {
            readLock.unlock();
        }

        for ( S s : values )
            f.apply( s );
    }

    private S findFirst(final int index) {
        for ( ;; ) {
            S retval;
            for ( int i = index; i < index + maxSegmentsCount; i++ ) {
                int idx = ( i % maxSegmentsCount );
                retval = segments.get( Integer.valueOf( idx ) );
                if ( retval != null )
                    return retval;
            }
        }
    }
}
