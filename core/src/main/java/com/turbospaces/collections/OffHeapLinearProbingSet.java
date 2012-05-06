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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mapping.model.MutablePersistentEntity;
import org.springframework.util.ObjectUtils;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.api.SpaceExpirationListener;
import com.turbospaces.core.SpaceUtility;
import com.turbospaces.offmemory.ByteArrayPointer;
import com.turbospaces.serialization.PropertiesSerializer;

/**
 * Off-heap linear probing map which uses linear probing hash algorithm for collision handling.</p>
 * 
 * Definitely there is a lot of stuff to do in terms of multi-threading access optimizations, but just right now we are
 * concentrated on memory efficiency as highest priority.</p>
 * 
 * @since 0.1
 */
@SuppressWarnings("serial")
@ThreadSafe
public class OffHeapLinearProbingSet extends ReentrantReadWriteLock implements OffHeapHashSet {
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
    public OffHeapLinearProbingSet(final int initialCapacity,
                                   final SpaceConfiguration configuration,
                                   final MutablePersistentEntity<?, ?> mutablePersistentEntity) {
        this.configuration = configuration;
        this.mutablePersistentEntity = mutablePersistentEntity;

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

    private Object get(final Object key,
                       final boolean asPointer) {
        final int index = hash2index( key );
        final Lock lock = readLock();
        boolean expired = false;
        ByteBuffer buffer = null;
        long ttl = 0;

        lock.lock();
        try {
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
            if ( expired ) {
                remove( key );
                notifyExpired( buffer, ttl );
            }
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
        int length = 0;
        final Lock lock = writeLock();

        lock.lock();
        try {
            int i = hash2index( key );
            for ( ; addresses[i] != 0; i = ( i + 1 ) % m ) {
                final byte[] serializedData = ByteArrayPointer.getEntityState( addresses[i] );
                ByteBuffer buffer = ByteBuffer.wrap( serializedData );
                if ( keyEquals( key, buffer ) ) {
                    length = ByteArrayPointer.getBytesOccupied( addresses[i] );
                    SpaceUtility.releaseMemory( addresses[i] );
                    break;
                }
            }

            if ( length == 0 )
                return length;

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
        return length;
    }

    @SuppressWarnings("unchecked")
    private void notifyExpired(final ByteBuffer buffer,
                               final long ttl) {
        assert buffer != null;
        assert ttl > 0;

        buffer.clear();
        // do in background
        configuration.getExecutorService().submit( new Runnable() {
            @Override
            public void run() {
                try {
                    SpaceExpirationListener expirationListener = configuration.getExpirationListener();
                    if ( expirationListener != null ) {
                        boolean retrieveAsEntity = expirationListener.retrieveAsEntity();
                        if ( retrieveAsEntity ) {
                            Object readObjectData = serializer.readObjectData( buffer, mutablePersistentEntity.getType() );
                            expirationListener.handleNotification( readObjectData, mutablePersistentEntity.getType(), ttl );
                        }
                        else
                            expirationListener.handleNotification( buffer, mutablePersistentEntity.getType(), ttl );
                    }
                }
                catch ( RuntimeException e ) {
                    logger.error( "unable to properly notify expiration listener", e );
                }
            }
        } );

    }

    private boolean keyEquals(final Object key,
                              final ByteBuffer buffer) {
        Object otherKey = serializer.readID( buffer );
        return ObjectUtils.nullSafeEquals( key, otherKey );
    }

    private int hash2index(final Object key) {
        return ( SpaceUtility.hash( key.hashCode() ) & Integer.MAX_VALUE ) % m;
    }

    private void resize(final int capacity) {
        Preconditions.checkArgument( capacity <= MAXIMUM_CAPACITY );

        OffHeapLinearProbingSet temp = new OffHeapLinearProbingSet( capacity, configuration, mutablePersistentEntity );
        for ( int i = 0; i < m; i++ )
            if ( addresses[i] != 0 ) {
                Object id = serializer.readID( ByteBuffer.wrap( ByteArrayPointer.getEntityState( addresses[i] ) ) );
                temp.put( id, addresses[i], null );
            }
        addresses = temp.addresses;
        m = temp.m;
    }

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
    public void afterPropertiesSet() {}

    @Override
    public String toString() {
        int size = 0;
        for ( int i = 0; i < m; i++ )
            if ( addresses[i] != 0 )
                size++;
        return Objects.toStringHelper( this ).add( "size", size ).toString();
    }
}
