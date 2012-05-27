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
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ObjectUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.turbospaces.api.CacheEvictionPolicy;
import com.turbospaces.api.SpaceExpirationListener;
import com.turbospaces.core.JVMUtil;
import com.turbospaces.model.CacheStoreEntryWrapper;
import com.turbospaces.offmemory.ByteArrayPointer;
import com.turbospaces.serialization.MatchingSerializer;

/**
 * Off-heap linear probing segment which uses linear probing hash algorithm for collision handling .</p>
 * 
 * Definitely there is a lot of stuff to do in terms of multi-threading access optimizations, but just right now we are
 * concentrated on memory efficiency as highest priority.</p>
 * 
 * This is low-level segment (small part of off-heap linear probing set) and generally it is package private class
 * only.</p>
 * 
 * @since 0.1
 * @see OffHeapLinearProbingSet
 */
@SuppressWarnings("rawtypes")
@ThreadSafe
class OffHeapLinearProbingSegment extends ReentrantReadWriteLock implements OffHeapHashSet {
    private static final long serialVersionUID = -9179258635918662649L;

    /**
     * Default segment's capacity = <strong>8.</strong>
     */
    static final int DEFAULT_SEGMENT_CAPACITY = 1 << 3;
    /**
     * The maximum expected capacity to work best for both matchByTemplate and matchById operations of
     * <strong>16384</code>.
     */
    static final int MAX_SEGMENT_CAPACITY = 1 << 14;

    private final CacheEvictionPolicy evictionPolicy;
    private final ExecutorService executorService;
    private final Logger logger = LoggerFactory.getLogger( getClass() );
    private final MatchingSerializer<?> serializer;
    private SpaceExpirationListener[] expirationListeners;
    private final Random random = new Random();
    private Ordering<EvictionEntry> evictionComparator;

    private int n;
    private int m;
    private long addresses[];

    /**
     * create new OffHeapHashMap with default initial capacity.
     * 
     * @param serializer
     *            persistent entity serializer
     * @param executorService
     *            this is for asynchronous cache expiration notification
     * @param evictionPolicy
     *            optional e
     */
    public OffHeapLinearProbingSegment(final MatchingSerializer<?> serializer,
                                       final ExecutorService executorService,
                                       final CacheEvictionPolicy evictionPolicy) {
        this( DEFAULT_SEGMENT_CAPACITY, serializer, executorService, evictionPolicy );
    }

    @VisibleForTesting
    OffHeapLinearProbingSegment(final int initialCapacity,
                                final MatchingSerializer<?> serializer,
                                final ExecutorService executorService,
                                final CacheEvictionPolicy evictionPolicy) {
        this.evictionPolicy = evictionPolicy;
        this.serializer = Preconditions.checkNotNull( serializer );
        this.executorService = Preconditions.checkNotNull( executorService );

        this.m = initialCapacity;
        this.addresses = new long[m];

        evictionComparator = Ordering.from( new Comparator<EvictionEntry>() {
            @Override
            public int compare(final EvictionEntry o1,
                               final EvictionEntry o2) {
                for ( ;; )
                    if ( evictionPolicy == CacheEvictionPolicy.LRU )
                        return ( o1.lastAccessTime < o2.lastAccessTime ? -1 : ( o1.lastAccessTime == o2.lastAccessTime ? 0 : 1 ) );
                    else if ( evictionPolicy == CacheEvictionPolicy.FIFO )
                        return ( o1.creationTimestamp < o2.creationTimestamp ? -1 : ( o1.creationTimestamp == o2.creationTimestamp ? 0 : 1 ) );
                    else if ( evictionPolicy == CacheEvictionPolicy.LFU )
                        return ( o1.accessTimes < o2.accessTimes ? -1 : ( o1.accessTimes == o2.accessTimes ? 0 : 1 ) );
            }
        } );
    }

    @Override
    public void setExpirationListeners(final SpaceExpirationListener... expirationListeners) {
        this.expirationListeners = expirationListeners;
    }

    @Override
    public boolean contains(final Object key) {
        return get( key, false ) != null;
    }

    @Override
    public ByteArrayPointer getAsPointer(final Object key) {
        return (ByteArrayPointer) get( key, true );
    }

    @Override
    public ByteBuffer getAsSerializedData(final Object key) {
        return (ByteBuffer) get( key, false );
    }

    @Override
    public List<ByteArrayPointer> match(final CacheStoreEntryWrapper template) {
        final Lock lock = readLock();
        List<ByteArrayPointer> matchedEntries = null;
        List<ExpiredEntry> expiredEntries = null;

        lock.lock();
        try {
            for ( int i = 0; i < m; i++ ) {
                long address = addresses[i];
                if ( address != 0 ) {
                    final byte[] serializedData = ByteArrayPointer.getEntityState( address );
                    final ByteBuffer buffer = ByteBuffer.wrap( serializedData );
                    final boolean matches = serializer.matches( buffer, template );

                    if ( ByteArrayPointer.isExpired( address ) ) {
                        if ( expiredEntries == null )
                            expiredEntries = Lists.newLinkedList();
                        ExpiredEntry expiredEntry = new ExpiredEntry( buffer, serializer.readID( buffer ), ByteArrayPointer.getTimeToLive( address ) );
                        expiredEntries.add( expiredEntry );
                        continue;
                    }

                    if ( matches ) {
                        if ( matchedEntries == null )
                            matchedEntries = Lists.newLinkedList();
                        buffer.clear();
                        matchedEntries.add( new ByteArrayPointer( address, buffer ) );
                        ByteArrayPointer.updateLastAccessTime( address, System.currentTimeMillis() );
                    }
                }
            }
        }
        finally {
            lock.unlock();
            if ( expiredEntries != null )
                for ( ExpiredEntry entry : expiredEntries )
                    // potentially another thread removed entry? check if bytesOccupied > 0
                    if ( remove( entry.id ) > 0 )
                        notifyExpired( entry );
        }
        return matchedEntries;
    }

    private Object get(final Object key,
                       final boolean asPointer) {
        final Lock lock = readLock();

        boolean expired = false;
        ByteBuffer buffer = null;
        int ttl = 0;

        lock.lock();
        try {
            // get the hash index first
            int index = hash2index( key );

            // use linear probing iteration starting at hash-index until not-zero array's element
            for ( int i = index; addresses[i] != 0; i = ( i + 1 ) % m ) {
                long address = addresses[i];
                byte[] serializedData = ByteArrayPointer.getEntityState( address );
                buffer = ByteBuffer.wrap( serializedData );
                // check whether key equals key from byte buffer's content
                if ( keyEquals( key, buffer ) ) {
                    // and finally validate that entity's state
                    if ( ByteArrayPointer.isExpired( address ) ) {
                        expired = true;
                        ttl = ByteArrayPointer.getTimeToLive( address );
                        return null;
                    }
                    ByteArrayPointer.updateLastAccessTime( address, System.currentTimeMillis() );
                    return asPointer ? new ByteArrayPointer( address, buffer ) : buffer;
                }
            }
        }
        finally {
            // immediately un-lock read lock in order to acquire write lock
            lock.unlock();
            if ( expired )
                // potentially another thread removed entry? check if bytesOccupied > 0
                if ( remove( key ) > 0 )
                    // and now raise expiration event
                    notifyExpired( new ExpiredEntry( buffer, serializer.readID( buffer ), ttl ) );
        }
        return null;
    }

    @Override
    public int put(final Object key,
                   final ByteArrayPointer value) {
        return put( key, 0, value );
    }

    private int put(final Object key,
                    final long address,
                    final ByteArrayPointer p) {
        final Lock lock = writeLock();
        lock.lock();
        try {
            // probably re-size immediately
            if ( n >= m / 2 )
                resize( 2 * m );

            int i;
            // use linear probing iteration starting at hash-index until not-zero array's element
            for ( i = hash2index( key ); addresses[i] != 0; i = ( i + 1 ) % m ) {
                byte[] serializedData = ByteArrayPointer.getEntityState( addresses[i] );
                ByteBuffer buffer = ByteBuffer.wrap( serializedData );
                // check whether key equals key from byte buffer's content
                if ( keyEquals( key, buffer ) ) {
                    // so this is override for given key
                    int length = ByteArrayPointer.getBytesOccupied( addresses[i] );
                    if ( p != null )
                        addresses[i] = p.rellocateAndDump( addresses[i] );
                    else {
                        JVMUtil.releaseMemory( addresses[i] );
                        addresses[i] = address;
                    }
                    return length;
                }
            }

            // dump(flush)
            addresses[i] = p != null ? p.dumpAndGetAddress() : address;
            // increment the size
            n++;

            return 0;
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public int remove(final Object key) {
        int bytesOccupied = 0;
        final Lock lock = writeLock();
        lock.lock();
        try {
            // try to find entry with the key
            int i = hash2index( key );
            for ( ; addresses[i] != 0; i = ( i + 1 ) % m ) {
                byte[] serializedData = ByteArrayPointer.getEntityState( addresses[i] );
                ByteBuffer buffer = ByteBuffer.wrap( serializedData );
                // if the key equals - release memory
                if ( keyEquals( key, buffer ) ) {
                    bytesOccupied = ByteArrayPointer.getBytesOccupied( addresses[i] );
                    assert bytesOccupied > 0;
                    JVMUtil.releaseMemory( addresses[i] );
                    break;
                }
            }

            // in case we didn't find the address for key, abort just because nothing to do
            if ( bytesOccupied == 0 )
                return bytesOccupied;

            // immediately set the address to be zero
            addresses[i] = 0;

            // for each key that was inserted later we need to re-insert it back just
            // because that might prematurely terminate the search for a key that was inserted into the table later
            i = ( i + 1 ) % m;
            while ( addresses[i] != 0 ) {
                long addressToRedo = addresses[i];
                addresses[i] = 0;
                // first decrement size because this can cause resize
                n--;

                // and simply put it back
                Object id = serializer.readID( ByteBuffer.wrap( ByteArrayPointer.getEntityState( addressToRedo ) ) );
                put( id, addressToRedo, null );
                i = ( i + 1 ) % m;
            }
            // decrement size
            n--;
            // probably re-size
            if ( n > 0 && n <= m / 8 )
                resize( m / 2 );
        }
        finally {
            lock.unlock();
        }
        return bytesOccupied;
    }

    private void notifyExpired(final ExpiredEntry expiredEntry) {
        assert expiredEntry != null;
        expiredEntry.buffer.clear();
        if ( expirationListeners != null )
            // do in background
            executorService.execute( new Runnable() {
                @SuppressWarnings("unchecked")
                @Override
                public void run() {
                    Object obj2notify = null;
                    for ( SpaceExpirationListener expirationListener : expirationListeners )
                        try {
                            boolean retrieveAsEntity = expirationListener.retrieveAsEntity();
                            if ( retrieveAsEntity ) {
                                if ( obj2notify == null )
                                    obj2notify = serializer.read( expiredEntry.buffer );
                                expirationListener.handleNotification( obj2notify, expiredEntry.id, serializer.getType(), expiredEntry.ttl );
                            }
                            else
                                expirationListener.handleNotification( expiredEntry.buffer, expiredEntry.id, serializer.getType(), expiredEntry.ttl );
                        }
                        catch ( Exception e ) {
                            logger.error( "unable to properly notify expiration listener", e );
                        }
                }
            } );
    }

    /**
     * Resize off-heap set to new capacity(either bigger or smaller, doesn't matter). This method can be called only is
     * synchronous manner because will cause concurrency issues.
     * 
     * @param capacity
     *            new capacity
     */
    private void resize(final int capacity) {
        OffHeapLinearProbingSegment temp = new OffHeapLinearProbingSegment( capacity, serializer, executorService, evictionPolicy );
        for ( int i = 0; i < m; i++ ) {
            final long address = addresses[i];
            if ( address != 0 ) {
                final ByteBuffer buffer = ByteBuffer.wrap( ByteArrayPointer.getEntityState( address ) );
                final Object id = serializer.readID( buffer );
                if ( ByteArrayPointer.isExpired( address ) ) {
                    notifyExpired( new ExpiredEntry( buffer, id, ByteArrayPointer.getTimeToLive( address ) ) );
                    JVMUtil.releaseMemory( address );
                    addresses[i] = 0;
                    continue;
                }
                temp.put( id, address, null );
            }
        }
        addresses = temp.addresses;
        m = temp.m;
        // re-assign number of items because potentially we already skipped expired entries, but didn't decrement n.
        n = temp.n;
    }

    /**
     * @return size of segment, this is suitable method for unit-testing only
     */
    @Override
    public int size() {
        final Lock lock = readLock();
        lock.lock();
        try {
            return n;
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public void destroy() {
        final Lock lock = writeLock();
        lock.lock();
        try {
            for ( int i = 0; i < m; i++ ) {
                long address = addresses[i];
                if ( address != 0 ) {
                    if ( ByteArrayPointer.isExpired( address ) ) {
                        byte[] entityState = ByteArrayPointer.getEntityState( address );
                        ByteBuffer buffer = ByteBuffer.wrap( entityState );
                        notifyExpired( new ExpiredEntry( buffer, serializer.readID( buffer ), ByteArrayPointer.getTimeToLive( address ) ) );
                    }
                    JVMUtil.releaseMemory( address );
                    addresses[i] = 0;
                }
            }
            this.addresses = new long[DEFAULT_SEGMENT_CAPACITY];
            this.m = DEFAULT_SEGMENT_CAPACITY;
            this.n = 0;
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public void cleanUp() {
        List<ExpiredEntry> expiredEntries = null;
        final Lock lock = writeLock();
        lock.lock();
        try {
            // just iterate over all all element and find (without any actions) expired entries
            for ( int i = 0; i < m; i++ ) {
                long address = addresses[i];
                if ( address != 0 )
                    if ( ByteArrayPointer.isExpired( address ) ) {
                        if ( expiredEntries == null )
                            expiredEntries = Lists.newLinkedList();

                        byte[] entityState = ByteArrayPointer.getEntityState( address );
                        ByteBuffer buffer = ByteBuffer.wrap( entityState );
                        expiredEntries.add( new ExpiredEntry( buffer, serializer.readID( buffer ), ByteArrayPointer.getTimeToLive( address ) ) );
                    }
            }

            // now if there are any expired entries, remove them
            if ( expiredEntries != null ) {
                logger.trace( "detected {} items of {} for removal due to ttl expiration", expiredEntries.size(), n );
                for ( ExpiredEntry entry : expiredEntries )
                    // potentially another thread removed entry? check if bytesOccupied > 0
                    if ( remove( entry.id ) > 0 ) {
                        notifyExpired( entry );
                        logger.trace( "automatically removed expired entry with key {}", entry.id );
                    }
            }
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public int evictPercentage(final int percentage) {
        final Lock lock = writeLock();
        lock.lock();
        try {
            return evictElements( ( n * percentage ) / 100 );
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public int evictElements(final int elements) {
        final Lock lock = writeLock();
        lock.lock();
        int evicted = 0;
        try {
            int sizeBefore = n;
            cleanUp();
            evicted = sizeBefore - n;
            if ( evicted >= elements )
                return elements;

            switch ( evictionPolicy ) {
                case RANDOM: {
                    while ( evicted < elements && n > 0 ) {
                        int randomIndex = random.nextInt( m );
                        long address = addresses[randomIndex];
                        if ( address != 0 ) {
                            byte[] serializedData = ByteArrayPointer.getEntityState( address );
                            ByteBuffer buffer = ByteBuffer.wrap( serializedData );
                            Object key = serializer.readID( buffer );
                            int bytes = remove( key );
                            assert bytes > 0;
                            evicted++;
                        }
                    }
                    break;
                }
                case LRU: {
                    evicted = evictLruFifoLfu( elements, evicted );
                    break;
                }
                case LFU: {
                    evicted = evictLruFifoLfu( elements, evicted );
                    break;
                }
                case FIFO: {
                    evicted = evictLruFifoLfu( elements, evicted );
                    break;
                }
                case REJECT:
                    throw new IllegalStateException( "rejection policy is illegal for eviction" );
            }
        }
        finally {
            lock.unlock();
        }
        return evicted;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper( this ).add( "size", size() ).toString();
    }

    private static final class ExpiredEntry {
        private final ByteBuffer buffer;
        private final int ttl;
        private final Object id;

        private ExpiredEntry(final ByteBuffer buffer, final Object id, final int ttl) {
            super();
            this.buffer = buffer;
            this.ttl = ttl;
            this.id = id;
        }
    }

    private static final class EvictionEntry {
        private final Object key;
        private final long lastAccessTime;
        private final long accessTimes;
        private final long creationTimestamp;

        private EvictionEntry(final Object key, final long lastAccessTime, final long accessTimes, final long creationTimestamp) {
            super();
            this.key = key;
            this.lastAccessTime = lastAccessTime;
            this.accessTimes = accessTimes;
            this.creationTimestamp = creationTimestamp;
        }
    }

    private int evictLruFifoLfu(final int elements,
                                final int evictedNow) {
        int evicted = evictedNow;
        List<EvictionEntry> evictionCandidates = Lists.newLinkedList();
        for ( int i = 0; i < m; i++ ) {
            long address = addresses[i];
            if ( address != 0 ) {
                long lastAccessTime = ByteArrayPointer.getLastAccessTime( address );
                long creationTimestamp = ByteArrayPointer.getCreationTimestamp( address );
                long accessTimes = 0;
                byte[] serializedData = ByteArrayPointer.getEntityState( address );
                Object id = serializer.readID( ByteBuffer.wrap( serializedData ) );
                evictionCandidates.add( new EvictionEntry( id, lastAccessTime, accessTimes, creationTimestamp ) );
            }
        }
        List<EvictionEntry> greatestOf = evictionComparator.leastOf( evictionCandidates, Math.min( elements, n ) );
        for ( EvictionEntry evictionEntry : greatestOf )
            remove( evictionEntry.key );
        return evicted;
    }

    private boolean keyEquals(final Object key,
                              final ByteBuffer buffer) {
        return ObjectUtils.nullSafeEquals( key, serializer.readID( buffer ) );
    }

    private int hash2index(final Object key) {
        return ( JVMUtil.murmurRehash( key.hashCode() ) & Integer.MAX_VALUE ) % m;
    }
}
