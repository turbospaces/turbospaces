/**
 * Copyright (C) 2011 Andrey Borisov <aandrey.borisov@gmail.com>
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mapping.model.MutablePersistentEntity;
import org.springframework.util.ObjectUtils;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.api.SpaceExpirationListener;
import com.turbospaces.core.SpaceUtility;
import com.turbospaces.offmemory.ByteArrayPointer;
import com.turbospaces.serialization.PropertiesSerializer;
import com.turbospaces.spaces.CacheStoreEntryWrapper;

/**
 * Off-heap linear probing set which uses linear probing hash algorithm for collision handling.</p>
 * 
 * Definitely there is a lot of stuff to do in terms of multi-threading access optimizations, but just right now we are
 * concentrated on memory efficiency as highest priority.</p>
 * 
 * This is low-level segment (small part of off-heap linear probing set).</p>
 * 
 * @since 0.1
 * @see OffHeapLinearProbingSet
 */
@SuppressWarnings("serial")
@ThreadSafe
public class OffHeapLinearProbingSegment extends ReentrantReadWriteLock implements OffHeapHashSet {
    private final Logger logger = LoggerFactory.getLogger( getClass() );
    private final SpaceConfiguration configuration;
    private final MutablePersistentEntity<?, ?> mutablePersistentEntity;
    private final PropertiesSerializer serializer;

    private int n;
    private int m;
    private long addresses[];

    /**
     * create new OffHeapHashMap for give initialCapacity.
     * 
     * @param initialCapacity
     * @param configuration
     * @param mutablePersistentEntity
     */
    public OffHeapLinearProbingSegment(final int initialCapacity,
                                       final SpaceConfiguration configuration,
                                       final MutablePersistentEntity<?, ?> mutablePersistentEntity) {
        this.configuration = Preconditions.checkNotNull( configuration );
        this.mutablePersistentEntity = Preconditions.checkNotNull( mutablePersistentEntity );

        this.m = initialCapacity;
        this.addresses = new long[m];

        serializer = (PropertiesSerializer) configuration.getKryo().getRegisteredClass( mutablePersistentEntity.getType() ).getSerializer();
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
            for ( long address : addresses )
                if ( address != 0 ) {
                    final byte[] serializedData = ByteArrayPointer.getEntityState( address );
                    final ByteBuffer buffer = ByteBuffer.wrap( serializedData );
                    final boolean matches = serializer.match( buffer, template );

                    if ( ByteArrayPointer.isExpired( address ) ) {
                        if ( expiredEntries == null )
                            expiredEntries = Lists.newLinkedList();
                        ExpiredEntry expiredEntry = new ExpiredEntry( buffer, ByteArrayPointer.getTimeToLive( address ) );
                        expiredEntry.id = serializer.readID( buffer );
                        expiredEntries.add( expiredEntry );
                        continue;
                    }

                    if ( matches ) {
                        if ( matchedEntries == null )
                            matchedEntries = Lists.newLinkedList();
                        buffer.clear();
                        matchedEntries.add( new ByteArrayPointer( address, buffer ) );
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
        long ttl = 0;

        lock.lock();
        try {
            int index = hash2index( key );

            for ( int i = index; addresses[i] != 0; i = ( i + 1 ) % m ) {
                final byte[] serializedData = ByteArrayPointer.getEntityState( addresses[i] );
                buffer = ByteBuffer.wrap( serializedData );
                if ( keyEquals( key, buffer ) ) {
                    if ( ByteArrayPointer.isExpired( addresses[i] ) ) {
                        expired = true;
                        ttl = ByteArrayPointer.getTimeToLive( addresses[i] );
                        return null;
                    }
                    return asPointer ? new ByteArrayPointer( addresses[i], buffer ) : buffer;
                }
            }
        }
        finally {
            lock.unlock();
            if ( expired )
                // potentially another thread removed entry? check if bytesOccupied > 0
                if ( remove( key ) > 0 )
                    notifyExpired( new ExpiredEntry( buffer, ttl ) );
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
            if ( n >= m / 2 )
                resize( 2 * m );

            int i;
            for ( i = hash2index( key ); addresses[i] != 0; i = ( i + 1 ) % m ) {
                final byte[] serializedData = ByteArrayPointer.getEntityState( addresses[i] );
                ByteBuffer buffer = ByteBuffer.wrap( serializedData );
                if ( keyEquals( key, buffer ) ) {
                    int length = ByteArrayPointer.getBytesOccupied( addresses[i] );
                    if ( p != null )
                        addresses[i] = p.dumpAt( addresses[i] );
                    else {
                        SpaceUtility.releaseMemory( addresses[i] );
                        addresses[i] = address;
                    }
                    return length;
                }
            }

            addresses[i] = p != null ? p.dump() : address;
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
            int i = hash2index( key );
            for ( ; addresses[i] != 0; i = ( i + 1 ) % m ) {
                final byte[] serializedData = ByteArrayPointer.getEntityState( addresses[i] );
                ByteBuffer buffer = ByteBuffer.wrap( serializedData );
                if ( keyEquals( key, buffer ) ) {
                    bytesOccupied = ByteArrayPointer.getBytesOccupied( addresses[i] );
                    SpaceUtility.releaseMemory( addresses[i] );
                    break;
                }
            }

            if ( bytesOccupied == 0 )
                return bytesOccupied;

            addresses[i] = 0;
            i = ( i + 1 ) % m;
            while ( addresses[i] != 0 ) {
                long addressToRedo = addresses[i];
                addresses[i] = 0;
                n--;

                Object id = serializer.readID( ByteBuffer.wrap( ByteArrayPointer.getEntityState( addressToRedo ) ) );
                put( id, addressToRedo, null );
                i = ( i + 1 ) % m;
            }
            n--;
            if ( n > 0 && n == m / 8 )
                resize( m / 2 );
        }
        finally {
            lock.unlock();
        }
        return bytesOccupied;
    }

    @SuppressWarnings("unchecked")
    private void notifyExpired(final ExpiredEntry expiredEntry) {
        assert expiredEntry != null;

        expiredEntry.buffer.clear();
        // do in background
        configuration.getExecutorService().submit( new Runnable() {
            @Override
            public void run() {
                try {
                    SpaceExpirationListener expirationListener = configuration.getExpirationListener();
                    if ( expirationListener != null ) {
                        boolean retrieveAsEntity = expirationListener.retrieveAsEntity();
                        if ( retrieveAsEntity ) {
                            Object readObjectData = serializer.readObjectData( expiredEntry.buffer, mutablePersistentEntity.getType() );
                            expirationListener.handleNotification( readObjectData, mutablePersistentEntity.getType(), expiredEntry.ttl );
                        }
                        else
                            expirationListener.handleNotification( expiredEntry.buffer, mutablePersistentEntity.getType(), expiredEntry.ttl );
                    }
                }
                catch ( Exception e ) {
                    logger.error( "unable to properly notify expiration listener", e );
                    Throwables.propagateIfPossible( e );
                }
            }
        } );

    }

    private boolean keyEquals(final Object key,
                              final ByteBuffer buffer) {
        Object bufferKey = serializer.readID( buffer );
        return ObjectUtils.nullSafeEquals( key, bufferKey );
    }

    private int hash2index(final Object key) {
        return ( SpaceUtility.hash( key.hashCode() ) & Integer.MAX_VALUE ) % m;
    }

    private void resize(final int capacity) {
        OffHeapLinearProbingSegment temp = new OffHeapLinearProbingSegment( capacity, configuration, mutablePersistentEntity );
        for ( int i = 0; i < m; i++ )
            if ( addresses[i] != 0 ) {
                Object id = serializer.readID( ByteBuffer.wrap( ByteArrayPointer.getEntityState( addresses[i] ) ) );
                temp.put( id, addresses[i], null );
            }
        addresses = temp.addresses;
        m = temp.m;
    }

    @Override
    public void afterPropertiesSet() {}

    @Override
    public void destroy() {
        final Lock lock = writeLock();

        lock.lock();
        try {
            for ( int i = 0; i < m; i++ )
                if ( addresses[i] != 0 ) {
                    SpaceUtility.releaseMemory( addresses[i] );
                    addresses[i] = 0;
                }
        }
        finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        final Lock lock = readLock();

        lock.lock();
        try {
            int size = 0;
            for ( int i = 0; i < m; i++ )
                if ( addresses[i] != 0 )
                    size++;
            return Objects.toStringHelper( this ).add( "size", size ).toString();
        }
        finally {
            lock.unlock();
        }
    }

    private static class ExpiredEntry {
        ByteBuffer buffer;
        long ttl;
        Object id;

        private ExpiredEntry(final ByteBuffer buffer, final long ttl) {
            super();
            this.buffer = buffer;
            this.ttl = ttl;
        }
    }
}
