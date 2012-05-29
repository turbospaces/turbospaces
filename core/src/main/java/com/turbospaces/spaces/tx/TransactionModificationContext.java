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
package com.turbospaces.spaces.tx;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.turbospaces.api.JSpace;
import com.turbospaces.api.SpaceNotificationListener;
import com.turbospaces.core.SpaceUtility;
import com.turbospaces.model.CacheStoreEntryWrapper;
import com.turbospaces.offmemory.ByteArrayPointer;
import com.turbospaces.offmemory.IndexManager;
import com.turbospaces.spaces.EntryKeyLockQuard;
import com.turbospaces.spaces.NotificationContext;
import com.turbospaces.spaces.SpaceModifiers;
import com.turbospaces.spaces.SpaceStore;

/**
 * buffer for space modifications. this is something that captures writes/takes to/from space under particular
 * transaction(actually this is transaction log). </p>
 * 
 * it is somehow differs from standard approach used by relations databases because RDBMS writes changes immediately(and
 * handles rollback via redo logs). in our case the master data is being updated only at the end of transaction and
 * rollback is very chip operation - just discard changes(forget), there is no need to restore data from transaction
 * redo logs.
 * 
 * @since 0.1
 */
public final class TransactionModificationContext {
    private static final Logger LOGGER = LoggerFactory.getLogger( TransactionModificationContext.class );
    private static final AtomicLong IDS = new AtomicLong();

    /**
     * map of added/modified space entries
     * 
     * @see JSpace#WRITE_ONLY
     * @see JSpace#WRITE_OR_UPDATE
     * @see JSpace#UPDATE_ONLY
     */
    private final Map<EntryKeyLockQuard, WriteTakeEntry> writes = Maps.newHashMap();
    /**
     * map of removed space entries
     * 
     * @see JSpace#EVICT_ONLY
     * @see JSpace#TAKE_ONLY
     */
    private final Map<EntryKeyLockQuard, WriteTakeEntry> takes = Maps.newHashMap();
    /**
     * map of exclusively locked keys
     * 
     * @see JSpace#EXCLUSIVE_READ_LOCK
     */
    private final Set<EntryKeyLockQuard> exclusiveReads = Sets.newHashSet();

    /**
     * unique identifier of transaction
     */
    private final long transactionId;
    /**
     * indicates that this transaction modification created for remote client(those behaves as proxy)
     */
    private boolean proxyMode;

    /**
     * create new transaction modification context and assign auto-generated ID.
     */
    public TransactionModificationContext() {
        this.transactionId = IDS.incrementAndGet();
    }

    /**
     * check whether transaction contains any write event for particular primary key(key wrapper).
     * 
     * @param uniqueIdentifier
     *            primary key
     * @return true if transaction modification context already has write(insert) associated with transaction by given
     *         key(wrapper).
     */
    public boolean hasWrite(final EntryKeyLockQuard uniqueIdentifier) {
        return getWrites().containsKey( uniqueIdentifier );
    }

    /**
     * add delete event to the context of current transaction, remove any previous writes for the same ID from the
     * current transaction.
     * 
     * @param guard
     *            key lock guardian
     * @param value
     *            write entry wrapper
     */
    public void addTake(final EntryKeyLockQuard guard,
                        final WriteTakeEntry value) {
        getWrites().remove( guard );
        getTakes().put( guard, value );
    }

    /**
     * add write(or probably write-or-update or just update) event to the context of current transaction, remove any
     * previous deletes by the same ID from the transaction.
     * 
     * @param guard
     *            key lock guardian
     * @param value
     *            write entry wrapper
     */
    public void addWrite(final EntryKeyLockQuard guard,
                         final WriteTakeEntry value) {
        getWrites().put( guard, value );
        getTakes().remove( guard );
    }

    /**
     * add exclusive read lock event to the context of current transaction
     * 
     * @param uniqueIdentifier
     *            primary key
     */
    public void addExclusiveReadLock(final EntryKeyLockQuard uniqueIdentifier) {
        getExclusiveReads().add( uniqueIdentifier );
    }

    /**
     * get byte array pointer associated with write/take for the given key if any or fetch byte array pointer from index
     * manager (if there is no direct key association with-in transaction).
     * 
     * @param guard
     *            key lock guardian
     * @param indexManager
     *            space index manager
     * @return byte array pointer or <code>null</code>
     */
    public ByteArrayPointer getPointer(final EntryKeyLockQuard guard,
                                       final IndexManager indexManager) {
        if ( getTakes().containsKey( guard ) )
            return null;
        if ( getWrites().containsKey( guard ) )
            return getWrites().get( guard ).getPointer();
        return (ByteArrayPointer) indexManager.getByUniqueIdentifier( guard.getKey(), true );
    }

    /**
     * get byte array pointer associated with write/take for the given key if any or fetch byte array pointer from index
     * manager (if there is no direct key association with-in transaction).
     * 
     * @param key
     *            primary key lock guardian
     * @param indexManager
     *            space index manager
     * @return byte array pointer or <code>null</code>
     */
    public ByteBuffer getPointerData(final Object key,
                                     final IndexManager indexManager) {
        if ( !getTakes().isEmpty() )
            for ( EntryKeyLockQuard keyGuard : getTakes().keySet() )
                if ( ObjectUtils.nullSafeEquals( keyGuard.getKey(), key ) )
                    return null;

        if ( !getWrites().isEmpty() )
            for ( Entry<EntryKeyLockQuard, WriteTakeEntry> next : getWrites().entrySet() )
                if ( ObjectUtils.nullSafeEquals( next.getKey().getKey(), key ) )
                    return next.getValue().getPointer().getSerializedDataBuffer();

        return (ByteBuffer) indexManager.getByUniqueIdentifier( key, false );
    }

    /**
     * @return true if there are writes/changes/takes associated with current transaction modification context.
     */
    public boolean isDirty() {
        return !getWrites().isEmpty() || !getTakes().isEmpty() || !getExclusiveReads().isEmpty();
    }

    /**
     * synchronize pending modification with off-heap memory manager, notify space notification subscribers(listeners).
     * 
     * @param memoryManager
     *            space store
     * @param notificationContext
     *            space notification context
     */
    public void flush(final SpaceStore memoryManager,
                      final Set<NotificationContext> notificationContext) {
        sync( memoryManager, notificationContext, true );
    }

    @SuppressWarnings("javadoc")
    public void flush(final SpaceStore memoryManager) {
        flush( memoryManager, null );
    }

    /**
     * discard changes made by the current transaction , notify space notification subscribers.
     * 
     * @param memoryManager
     *            space store manager
     */
    public void discard(final SpaceStore memoryManager) {
        sync( memoryManager, null, false );
    }

    private void sync(final SpaceStore memoryManager,
                      final Set<NotificationContext> notificationContext,
                      final boolean applyDiscard) {
        LOGGER.debug( "synchronizing transaction {} wih offheap cache store. commit/rollback = {}", getTransactionId(), applyDiscard ? "C" : "R" );
        try {
            memoryManager.sync( this, applyDiscard );
            if ( !CollectionUtils.isEmpty( notificationContext ) )
                for ( NotificationContext item : notificationContext ) {
                    SpaceNotificationListener listener = item.getListener();
                    boolean matchById = SpaceModifiers.isMatchById( item.getModifier() );
                    boolean returnAsBytes = SpaceModifiers.isReturnAsBytes( item.getModifier() );
                    CacheStoreEntryWrapper template = item.getTemplateEntry();

                    if ( matchById ) {
                        notifyById( getWrites(), template.getId(), listener, memoryManager, template.getBean(), returnAsBytes );
                        notifyById( getTakes(), template.getId(), listener, memoryManager, template.getBean(), returnAsBytes );
                    }
                    else {
                        notifyByTemplate( getWrites(), listener, template, memoryManager, returnAsBytes );
                        notifyByTemplate( getTakes(), listener, template, memoryManager, returnAsBytes );
                    }
                }
        }
        finally {
            clear();
        }
    }

    private static void notifyByTemplate(final Map<EntryKeyLockQuard, WriteTakeEntry> map,
                                         final SpaceNotificationListener listener,
                                         final CacheStoreEntryWrapper template,
                                         final SpaceStore memoryManager,
                                         final boolean returnAsBytes) {
        if ( !CollectionUtils.isEmpty( map ) )
            for ( final WriteTakeEntry entry : map.values() )
                if ( entry.getObj().getClass().isAssignableFrom( template.getPersistentEntity().getOriginalPersistentEntity().getType() ) ) {
                    Object[] templatePropertyValues = template.asPropertyValuesArray();
                    Object[] entryPropertyValues = entry.getPropertyValues();

                    if ( SpaceUtility.macthesByPropertyValues( templatePropertyValues, entryPropertyValues ) ) {
                        assert entry.getObj() != null;
                        memoryManager.getSpaceConfiguration().getListeningExecutorService().execute( new Runnable() {
                            @Override
                            public void run() {
                                listener.handleNotification(
                                        returnAsBytes ? entry.getPointer().getSerializedDataBuffer() : entry.getObj(),
                                        entry.getSpaceOperation() );
                            }
                        } );
                    }
                }
    }

    private static void notifyById(final Map<EntryKeyLockQuard, WriteTakeEntry> map,
                                   final Object uniqueIdentifier,
                                   final SpaceNotificationListener listener,
                                   final SpaceStore memoryManager,
                                   final Object template,
                                   final boolean returnAsBytes) {
        if ( !CollectionUtils.isEmpty( map ) )
            for ( Entry<EntryKeyLockQuard, WriteTakeEntry> next : map.entrySet() ) {
                Object keyCandidate = next.getKey().getKey();
                final WriteTakeEntry entry = next.getValue();

                if ( ObjectUtils.nullSafeEquals( keyCandidate, uniqueIdentifier )
                        && ObjectUtils.nullSafeEquals( entry.getPersistentEntity().getOriginalPersistentEntity().getType(), template.getClass() ) ) {
                    assert entry.getObj() != null;
                    memoryManager.getSpaceConfiguration().getListeningExecutorService().execute( new Runnable() {
                        @Override
                        public void run() {
                            listener.handleNotification(
                                    returnAsBytes ? entry.getPointer().getSerializedDataBuffer() : entry.getObj(),
                                    entry.getSpaceOperation() );
                        }
                    } );
                }
            }
    }

    /**
     * clear all writes/changes/takes/exclusive reads.
     */
    public void clear() {
        if ( !getWrites().isEmpty() )
            getWrites().clear();
        if ( !getTakes().isEmpty() )
            getTakes().clear();
        if ( !getExclusiveReads().isEmpty() )
            getExclusiveReads().clear();
    }

    /**
     * get the transaction id. you can use this transaction id for different kind of locks where simple long/int field
     * is required.
     * 
     * @return ID uniquely representing the modification context.
     */
    public long getTransactionId() {
        return transactionId;
    }

    /**
     * @return if works in proxy mode
     * @see #setProxyMode(boolean)
     */
    public boolean isProxyMode() {
        return proxyMode;
    }

    /**
     * set whether transaction modification context reflects remote transaction, those working in proxy mode
     * 
     * @param proxyMode
     *            whether this is remote transaction or local
     */
    public void setProxyMode(final boolean proxyMode) {
        this.proxyMode = proxyMode;
    }

    /**
     * get collection of inserts associated with the current transaction (id->value)
     * 
     * @return map of inserts
     */
    public Map<EntryKeyLockQuard, WriteTakeEntry> getWrites() {
        return writes;
    }

    /**
     * get collection of deletes associated with the current transaction (id->value)
     * 
     * @return map of deletes
     */
    public Map<EntryKeyLockQuard, WriteTakeEntry> getTakes() {
        return takes;
    }

    /**
     * get collection of exclusively locked reads associated with the current transaction
     * 
     * @return map of exclusively locked id-s
     */
    public Set<EntryKeyLockQuard> getExclusiveReads() {
        return exclusiveReads;
    }
}
