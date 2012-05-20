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
package com.turbospaces.offmemory;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

import com.esotericsoftware.kryo.ObjectBuffer;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.api.SpaceOperation;
import com.turbospaces.core.CacheStatistics;
import com.turbospaces.core.JVMUtil;
import com.turbospaces.core.SpaceUtility;
import com.turbospaces.model.BO;
import com.turbospaces.model.CacheStoreEntryWrapper;
import com.turbospaces.pool.ObjectPool;
import com.turbospaces.spaces.EntryKeyLockQuard;
import com.turbospaces.spaces.KeyLocker;
import com.turbospaces.spaces.SpaceCapacityRestrictionHolder;
import com.turbospaces.spaces.SpaceModifiers;
import com.turbospaces.spaces.SpaceStore;
import com.turbospaces.spaces.tx.TransactionModificationContext;
import com.turbospaces.spaces.tx.WriteTakeEntry;

/**
 * This is the central class which is responsible for high-level jspace interactions orchestration (manage
 * write/take/read operations), guard of ACID jspace behavior via built-in synchronization mechanisms, protect jspace
 * from illegal concurrent modifications from parallel transactions. Another very important thing is managing of
 * off-heap operations, buffering direct byte array allocation which can exceed JVM memory
 * restrictions and allows to manipulate data outside garbage collector without GC pauses.</p>
 * 
 * This is <b>cache per class</b> off-heap buffer storage.
 * 
 * @since 0.1
 */
@ThreadSafe
public class OffHeapCacheStore implements SpaceStore {
    private final BO bo;
    private final SpaceConfiguration configuration;
    private final IndexManager indexManager;
    private final CacheStatistics statsCounter;
    private final KeyLocker lockManager;
    private final ObjectPool<ObjectBuffer> objectBufferPool;
    private final SpaceCapacityRestrictionHolder capacityRestriction;

    /**
     * create new off-heap memory buffer for the given configuration and particular entity class.
     * 
     * @param configuration
     *            jspace store configuration
     * @param entityClass
     *            target entity class that this instance has been created for
     * @param capacityRestriction
     *            store capacity restriction
     */
    @SuppressWarnings("unchecked")
    public OffHeapCacheStore(final SpaceConfiguration configuration,
                             final Class<?> entityClass,
                             final SpaceCapacityRestrictionHolder capacityRestriction) {
        this.configuration = configuration;
        this.capacityRestriction = capacityRestriction;
        this.indexManager = new IndexManager( configuration.getMappingContext().getPersistentEntity( entityClass ), configuration );
        this.statsCounter = new CacheStatistics();
        this.lockManager = SpaceUtility.parallelizedKeyLocker();
        this.objectBufferPool = JVMUtil.newObjectBufferPool();
        this.bo = configuration.boFor( entityClass );
    }

    @SuppressWarnings("unchecked")
    @Override
    public void sync(final TransactionModificationContext modificationContext,
                     final boolean apply) {
        int size = modificationContext.getWrites().size() + modificationContext.getTakes().size() + modificationContext.getExclusiveReads().size();
        Set<EntryKeyLockQuard> unlockKeys = size > 0 ? new HashSet<EntryKeyLockQuard>( size ) : Collections.EMPTY_SET;

        try {
            if ( !modificationContext.getWrites().isEmpty() )
                for ( Entry<EntryKeyLockQuard, WriteTakeEntry> entry : modificationContext.getWrites().entrySet() ) {
                    unlockKeys.add( entry.getKey() );
                    if ( apply ) {
                        WriteTakeEntry value = entry.getValue();
                        capacityRestriction.ensureCapacity( value.getPointer(), value.getObj() );
                        int prevBytesOccupation = indexManager.add( value.getObj(), value.getIdLockQuard(), value.getPointer() );
                        value.setSpaceOperation( prevBytesOccupation > 0 ? SpaceOperation.UPDATE : SpaceOperation.WRITE );
                        capacityRestriction.add( value.getPointer().bytesOccupied(), prevBytesOccupation );
                        statsCounter.increasePutsCount();
                    }
                    else
                        entry.getValue().getPointer().utilize();
                }

            if ( !modificationContext.getTakes().isEmpty() )
                for ( Entry<EntryKeyLockQuard, WriteTakeEntry> entry : modificationContext.getTakes().entrySet() ) {
                    unlockKeys.add( entry.getKey() );
                    if ( apply ) {
                        entry.getValue().setSpaceOperation( SpaceOperation.TAKE );
                        int bytesFreed = indexManager.takeByUniqueIdentifier( entry.getKey() );
                        capacityRestriction.remove( bytesFreed );
                        statsCounter.increaseTakesCount();
                    }
                }

            if ( !modificationContext.getExclusiveReads().isEmpty() )
                for ( EntryKeyLockQuard keyGuard : modificationContext.getExclusiveReads() )
                    if ( apply ) {
                        unlockKeys.add( keyGuard );
                        statsCounter.increaseExclusiveReadsCount();
                    }
        }
        finally {
            if ( !unlockKeys.isEmpty() ) {
                for ( EntryKeyLockQuard keyGuard : unlockKeys )
                    lockManager.writeUnlock( keyGuard, modificationContext.getTransactionId() );
                unlockKeys.clear();
            }
        }
    }

    @Override
    public void write(final CacheStoreEntryWrapper entry,
                      final TransactionModificationContext modificationContext,
                      final int timeToLive,
                      final int timeout,
                      final int modifier) {
        Object uniqueIdentifier = entry.getId();
        ObjectBuffer objectBuffer = objectBufferPool.borrowObject();
        objectBuffer.setKryo( configuration.getKryo() );

        boolean isWriteOnly = SpaceModifiers.isWriteOnly( modifier );
        boolean isUpdateOnly = SpaceModifiers.isUpdateOnly( modifier );

        /**
         * 1. acquire write lock guard to protected particular entity from concurrent modification
         * 2. raise duplicate key violation exception in write only modifier is used and entity already exists
         * 3. raise object retrieve exception if update_only modifier is used and there is no such entity in space
         * 4. allocate memory and write serialized state to the off-heap memory
         * 5. save write modification in scope of transaction modification context
         */

        EntryKeyLockQuard writeLockQuard = acquireKeyLock( uniqueIdentifier, modificationContext, timeout );

        boolean hasWriteInModificationContext = modificationContext.hasWrite( writeLockQuard );
        if ( isWriteOnly && ( hasWriteInModificationContext || indexManager.containsUniqueIdentifier( uniqueIdentifier ) ) )
            SpaceUtility.raiseDuplicateException( uniqueIdentifier, entry.getPersistentEntity().getOriginalPersistentEntity().getType() );
        if ( isUpdateOnly && !hasWriteInModificationContext && !indexManager.containsUniqueIdentifier( uniqueIdentifier ) )
            SpaceUtility.raiseObjectRetrieveFailureException( uniqueIdentifier, entry.getPersistentEntity().getOriginalPersistentEntity().getType() );

        ByteArrayPointer p = new ByteArrayPointer( entry.asSerializedData( objectBuffer ), entry.getBean(), timeToLive );
        modificationContext.addWrite( writeLockQuard, new WriteTakeEntry(
                entry.getBean(),
                entry.asPropertyValuesArray(),
                writeLockQuard,
                p,
                bo,
                configuration ) );

        objectBufferPool.returnObject( objectBuffer );
    }

    @Override
    public ByteBuffer[] fetch(final CacheStoreEntryWrapper template,
                              final TransactionModificationContext modificationContext,
                              final int timeout,
                              final int maxResults,
                              final int modifiers) {
        boolean isTakeOnly = SpaceModifiers.isTakeOnly( modifiers );
        boolean isExclusiveRead = SpaceModifiers.isExclusiveRead( modifiers );
        boolean isMatchById = SpaceModifiers.isMatchById( modifiers );
        boolean isEvictOnly = SpaceModifiers.isEvictOnly( modifiers );

        if ( isMatchById ) {
            final Object uniqueIdentifier = template.getId();
            ByteBuffer entryState = null;

            if ( isTakeOnly || isEvictOnly ) {
                EntryKeyLockQuard writeLockQuard = acquireKeyLock( uniqueIdentifier, modificationContext, timeout );
                ByteArrayPointer p = modificationContext.getPointer( writeLockQuard, indexManager );
                if ( p != null ) {
                    entryState = p.getSerializedDataBuffer();
                    modificationContext.addTake( writeLockQuard, new WriteTakeEntry( writeLockQuard, p, bo, configuration ) );
                }
                else
                    lockManager.writeUnlock( writeLockQuard, modificationContext.getTransactionId() );
            }
            else if ( isExclusiveRead ) {
                EntryKeyLockQuard exclusiveReadLockGuard = acquireKeyLock( uniqueIdentifier, modificationContext, timeout );
                ByteArrayPointer p = modificationContext.getPointer( exclusiveReadLockGuard, indexManager );
                if ( p != null ) {
                    entryState = p.getSerializedDataBuffer();
                    modificationContext.addExclusiveReadLock( exclusiveReadLockGuard );
                }
                else
                    lockManager.writeUnlock( exclusiveReadLockGuard, modificationContext.getTransactionId() );
            }
            else {
                entryState = modificationContext.getPointerData( uniqueIdentifier, indexManager );
                if ( entryState != null )
                    statsCounter.increaseHitsCount();
            }

            if ( entryState != null )
                return new ByteBuffer[] { entryState };
        }
        else {
            final List<ByteBuffer> l = Lists.newLinkedList();

            for ( Entry<EntryKeyLockQuard, WriteTakeEntry> entry : modificationContext.getWrites().entrySet() ) {
                EntryKeyLockQuard uniqueIdentifier = entry.getKey();
                ByteArrayPointer pointer = entry.getValue().getPointer();
                ByteBuffer data = pointer.getSerializedDataBuffer();
                matchByTemplate( modificationContext, data, uniqueIdentifier, template, l, modifiers, timeout );
                if ( l.size() == maxResults )
                    return l.toArray( new ByteBuffer[l.size()] );
            }

            return l.toArray( new ByteBuffer[l.size()] );
        }

        return null;
    }

    // TODO: un-used at the moment
    private void matchByTemplate(final TransactionModificationContext modificationContext,
                                 final ByteBuffer data,
                                 final Object uniqueIdentifier,
                                 final CacheStoreEntryWrapper template,
                                 final List<ByteBuffer> l,
                                 final int modifiers,
                                 final long timeout) {
        boolean isTakeOnly = SpaceModifiers.isTakeOnly( modifiers );
        boolean isExclusiveRead = SpaceModifiers.isExclusiveRead( modifiers );
        boolean isEvictOnly = SpaceModifiers.isEvictOnly( modifiers );

        boolean matches = configuration.getKryo().matchByTemplate( data, template );

        if ( matches )
            if ( isTakeOnly || isEvictOnly ) {
                EntryKeyLockQuard writeLockQuard = acquireKeyLock( uniqueIdentifier, modificationContext, timeout );
                ByteArrayPointer p = modificationContext.getPointer( writeLockQuard, indexManager );
                // if ( p != null )
                // modificationContext.addTake( writeLockQuard, WriteTakeEntry.valueOf( match, writeLockQuard, p, bo )
                // );
                // l.add( match );
            }
            else if ( isExclusiveRead ) {
                EntryKeyLockQuard exclusiveReadLockGuard = acquireKeyLock( uniqueIdentifier, modificationContext, timeout );
                ByteArrayPointer p = modificationContext.getPointer( exclusiveReadLockGuard, indexManager );
                if ( p != null )
                    modificationContext.addExclusiveReadLock( exclusiveReadLockGuard );
                // l.add( match );
            }
            else {
                ByteBuffer entityState = modificationContext.getPointerData( uniqueIdentifier, indexManager );
                if ( entityState != null )
                    statsCounter.increaseHitsCount();
                // l.add( match );
            }
    }

    private EntryKeyLockQuard acquireKeyLock(final Object key,
                                             final TransactionModificationContext modificationContext,
                                             final long timeout) {
        EntryKeyLockQuard writeLockGuard = lockManager.writeLock(
                key,
                modificationContext.getTransactionId(),
                timeout,
                !modificationContext.isProxyMode() );
        if ( writeLockGuard == null )
            SpaceUtility.raiseCannotAcquireLockException( key, timeout, true );
        return writeLockGuard;
    }

    @SuppressWarnings("javadoc")
    public CacheStatistics cacheStatisticsSnapshot() {
        CacheStatistics statistics = statsCounter.clone();
        statistics.setOffHeapBytesOccupied( indexManager.offHeapBytesOccuiped() );
        return statistics;
    }

    @SuppressWarnings("javadoc")
    public CacheStatistics resetCacheStatistics() {
        CacheStatistics clone = cacheStatisticsSnapshot();
        statsCounter.reset();
        return clone;
    }

    @Override
    public IndexManager getIndexManager() {
        return indexManager;
    }

    @Override
    public SpaceConfiguration getSpaceConfiguration() {
        return configuration;
    }

    @Override
    public void destroy() {
        indexManager.destroy();
    }

    @Override
    public void afterPropertiesSet() {
        indexManager.afterPropertiesSet();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper( this ).add( "indexManager", indexManager ).add( "lockManager", lockManager ).toString();
    }
}
