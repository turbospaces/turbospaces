package com.turbospaces.core;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;

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
public class ConsistentHasher<S> {
    private final Logger logger = LoggerFactory.getLogger( getClass() );

    private final SortedMap<Short, S> segments = new TreeMap<Short, S>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final short maxSegmentsCount;
    private final Supplier<S> segmentInstantiationFunction;

    /**
     * create consistent hasher with given {@code maxSegmentsCount} (maximum segments count to which this consistent
     * hasher can grow) and segment instantiation callback.
     * 
     * @param maxSegmentsCount
     * @param initialSegmentsCount
     * @param segmentInstantiationFunction
     */
    public ConsistentHasher(final short maxSegmentsCount, final short initialSegmentsCount, final Supplier<S> segmentInstantiationFunction) {
        this.maxSegmentsCount = maxSegmentsCount;
        this.segmentInstantiationFunction = segmentInstantiationFunction;

        List<S> newSegments = new ArrayList<S>( initialSegmentsCount );
        for ( int i = 0; i < initialSegmentsCount; i++ ) {
            S s = this.segmentInstantiationFunction.get();
            newSegments.add( s );
        }
        addSegments( newSegments );
        logger.trace( "segments = {}", segments );
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
                int hash = ( SpaceUtility.hash( System.identityHashCode( segment ) ) & Integer.MAX_VALUE ) % maxSegmentsCount;
                for ( int i = hash; i < hash + maxSegmentsCount; i++ ) {
                    short new_index = (short) ( i % maxSegmentsCount );
                    if ( !segments.containsKey( new_index ) ) {
                        segments.put( new_index, segment );
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
     * consistently calculate the proper segment for key using consistent hash algoritm.
     * 
     * @param key
     * @return proper segment(slot) for hash
     */
    public S segmentFor(final Object key) {
        final int hash = SpaceUtility.hash( key.hashCode() ) & Integer.MAX_VALUE;
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

    private S findFirst(final int index) {
        for ( ;; ) {
            S retval;
            for ( int i = index; i < index + maxSegmentsCount; i++ ) {
                short new_index = (short) ( i % maxSegmentsCount );
                retval = segments.get( new_index );
                if ( retval != null )
                    return retval;
            }
        }
    }
}
