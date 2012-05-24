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
package com.turbospaces.spaces;

import org.springframework.transaction.TransactionStatus;

import com.google.common.base.Optional;
import com.turbospaces.api.AbstractSpaceConfiguration;
import com.turbospaces.api.JSpace;
import com.turbospaces.api.SpaceNotificationListener;
import com.turbospaces.api.SpaceTopology;
import com.turbospaces.core.SpaceUtility;
import com.turbospaces.model.CacheStoreEntryWrapper;
import com.turbospaces.spaces.tx.SpaceTransactionHolder;

/**
 * This class defines simplistic methods for space interactions. In fact this class does not implement any of
 * fundamental methods, but rather defines simplistic methods and just delegates execution.</p>
 * 
 * You can use this class to create wrapper over actual JSpace implementation to get simplified space API.
 * 
 * @since 0.1
 */
public class SimplisticJSpace implements TransactionalJSpace {
    /**
     * constant meaning that there is no timeout(without timeout, immediate feedback) for operations (basically
     * <code>timeout=0</code>). If you are trying to insert entity and the entity is currently locked or being
     * inserted(but not committed) within context of parallel transaction, you will get exception immediately
     * without waiting for concurrent transaction commit/rollback.
     */
    public static final int WITHOUT_TIMEOUT = 0;

    /**
     * constant meaning that timeout is not achievable and you will be blocked forever in case of concurrency
     * update/write/take operations. Better to say not forever, but until concurrent transaction will free write (or
     * read exclusive) lock and current transaction obtain lock. This is something very typical for RDBMS.
     */
    public static final int WAIT_FOREVER_TIMEOUT = Integer.MAX_VALUE;

    private final TransactionalJSpace delegate;

    /**
     * wrap actual space implementation.
     * 
     * @param actualImplementation
     *            real implementation
     */
    public SimplisticJSpace(final TransactionalJSpace actualImplementation) {
        this.delegate = actualImplementation;
    }

    @Override
    public AbstractSpaceConfiguration getSpaceConfiguration() {
        return delegate.getSpaceConfiguration();
    }

    // write simplifications
    /**
     * the same as {@link #write(IBO, TransactionStatus, long, long, int)} with {@link JSpace#WRITE_OR_UPDATE} modifier.
     */
    @SuppressWarnings("javadoc")
    public void write(final Object entry,
                      final int timeToLive,
                      final int timeout) {
        write( entry, timeToLive, timeout, JSpace.WRITE_OR_UPDATE );
    }

    /**
     * the same as {@link #write(IBO, TransactionStatus, long, long, int)} with {@link JSpace#WRITE_OR_UPDATE} modifier
     * and <code>timeToLive={@link JSpace#LEASE_FOREVER} </code>.
     */
    @SuppressWarnings("javadoc")
    public void write(final Object entry,
                      final int timeout) {
        write( entry, JSpace.LEASE_FOREVER, timeout, JSpace.WRITE_OR_UPDATE );
    }

    /**
     * the same as {@link #write(IBO, TransactionStatus, long, long, int)} with {@link JSpace#WRITE_OR_UPDATE} modifier
     * and <code>timeToLive={@link JSpace#LEASE_FOREVER} and <code>timeout={@link #WAIT_FOREVER_TIMEOUT}</code>.
     */
    @SuppressWarnings("javadoc")
    public void write(final Object entry) {
        write( entry, JSpace.LEASE_FOREVER, WAIT_FOREVER_TIMEOUT, JSpace.WRITE_OR_UPDATE );
    }

    // read simplifications
    /**
     * the same as {@link #fetch(IBO, TransactionStatus, long, int, int)} with {@link JSpace#READ_ONLY} modifier.
     * you can treat this as read-only query by template.
     */
    @SuppressWarnings("javadoc")
    public Object[] read(final Object template,
                         final int timeout,
                         final int maxResults) {
        return fetch( template, timeout, maxResults, JSpace.READ_ONLY );
    }

    /**
     * the same as {@link #fetch(IBO, TransactionStatus, long, int, int)} with {@link JSpace#READ_ONLY} modifier and
     * <code>timeout={@link #WITHOUT_TIMEOUT}</code>.
     */
    @SuppressWarnings("javadoc")
    public Object[] read(final Object template,
                         final int maxResults) {
        return fetch( template, WITHOUT_TIMEOUT, maxResults, JSpace.READ_ONLY );
    }

    /**
     * the same as {@link #fetch(IBO, TransactionStatus, long, int, int)} with {@link JSpace#READ_ONLY} modifier and
     * <code>timeout={@link #WITHOUT_TIMEOUT}</code> and <code>maxElements={@link Integer#MAX_VALUE}</code>.
     */
    @SuppressWarnings("javadoc")
    public Object[] read(final Object template) {
        return fetch( template, WITHOUT_TIMEOUT, Integer.MAX_VALUE, JSpace.READ_ONLY );
    }

    /**
     * the same as {@link #fetch(IBO, TransactionStatus, long, int, int)} with {@link JSpace#READ_ONLY} +
     * {@link JSpace#MATCH_BY_ID} modifier and <code>maxElements=1</code>. </p>
     * 
     * This the most common method you would expect to find in most caching solutions except this method allows to pass
     * timeout as well (this may be useful if case of concurrent transaction holding {@link JSpace#EXCLUSIVE_READ_LOCK}
     * lock on matched object).
     */
    @SuppressWarnings({ "javadoc" })
    public <T> Optional<T> readByID(final Object id,
                                    final Class<T> clazz,
                                    final int timeout) {
        final CacheStoreEntryWrapper wrapper = CacheStoreEntryWrapper.readByIdValueOf( delegate.getSpaceConfiguration().boFor( clazz ), id );
        final Object[] objects = fetch( wrapper, timeout, 1, JSpace.READ_ONLY | JSpace.MATCH_BY_ID );
        return SpaceUtility.singleResult( objects );
    }

    /**
     * the same as {@link #fetch(IBO, TransactionStatus, long, int, int)} with {@link JSpace#READ_ONLY} +
     * {@link JSpace#MATCH_BY_ID} + {@link JSpace#EXCLUSIVE_READ_LOCK} modifier and <code>maxElements=1</code>. </p>
     * 
     * This method will acquire exclusive read lock and no other concurrent transactions will be able at least to read
     * this entity.
     */
    @SuppressWarnings({ "javadoc" })
    public <T> Optional<T> readExclusivelyByID(final Object id,
                                               final Class<T> clazz,
                                               final int timeout) {
        final CacheStoreEntryWrapper wrapper = CacheStoreEntryWrapper.readByIdValueOf( delegate.getSpaceConfiguration().boFor( clazz ), id );
        final Object[] objects = fetch( wrapper, timeout, 1, JSpace.READ_ONLY | JSpace.MATCH_BY_ID | JSpace.EXCLUSIVE_READ_LOCK );
        return SpaceUtility.singleResult( objects );
    }

    /**
     * the same as {@link #fetch(IBO, TransactionStatus, long, int, int)} with {@link JSpace#READ_ONLY} +
     * {@link JSpace#MATCH_BY_ID} modifier and <code>timeout={@link #WITHOUT_TIMEOUT}</code> and
     * <code>maxElements=1</code>. </p>
     * 
     * This the most common method you would expect to find in most caching solutions.
     */
    @SuppressWarnings({ "javadoc" })
    public <T> Optional<T> readByID(final Object id,
                                    final Class<T> clazz) {
        return readByID( id, clazz, WITHOUT_TIMEOUT );
    }

    // take modifiers
    /**
     * the same as {@link #fetch(IBO, TransactionStatus, long, int, int)} with {@link JSpace#TAKE_ONLY} modifier and
     * <code>maxElements={@link Integer#MAX_VALUE}</code>. </p>
     * 
     * You can treat this method as database delete except this methods allows you to delete by template and specify
     * timeout for delete operation.
     */
    @SuppressWarnings("javadoc")
    public Object[] take(final Object template,
                         final int timeout) {
        return fetch( template, timeout, Integer.MAX_VALUE, JSpace.TAKE_ONLY );
    }

    /**
     * the same as {@link #fetch(IBO, TransactionStatus, long, int, int)} with {@link JSpace#TAKE_ONLY} modifier and
     * <code>timeout={@link #WAIT_FOREVER_TIMEOUT}</code>. </p>
     * 
     * This is analog of database delete by table columns where tables columns are fields from template.
     */
    @SuppressWarnings("javadoc")
    public Object[] take(final Object template) {
        return fetch( template, WAIT_FOREVER_TIMEOUT, Integer.MAX_VALUE, JSpace.TAKE_ONLY );
    }

    /**
     * the same as {@link #fetch(IBO, TransactionStatus, long, int, int)} with {@link JSpace#TAKE_ONLY} +
     * {@link JSpace#MATCH_BY_ID} modifier and <code>maxElements=1</code>. </p>
     * 
     * This the most common method you would expect to find in most caching solutions(probably it is called removeById
     * or just remove with some key) except this method allows to pass timeout as well.
     */
    @SuppressWarnings({ "javadoc" })
    public <T> Optional<T> takeByID(final Object id,
                                    final Class<T> clazz,
                                    final int timeout) {
        final CacheStoreEntryWrapper wrapper = CacheStoreEntryWrapper.readByIdValueOf( delegate.getSpaceConfiguration().boFor( clazz ), id );
        final Object[] objects = fetch( wrapper, timeout, 1, JSpace.TAKE_ONLY | JSpace.MATCH_BY_ID );
        return SpaceUtility.singleResult( objects );
    }

    /**
     * the same as {@link #fetch(IBO, TransactionStatus, long, int, int)} with {@link JSpace#TAKE_ONLY} +
     * {@link JSpace#MATCH_BY_ID} modifier and <code>maxElements=1</code> where
     * <code>timeout={@link #WAIT_FOREVER_TIMEOUT}</code> </p>
     * 
     * This the most common method you would expect to find in most caching solutions(probably it is called removeById
     * or just remove with some key).
     */
    @SuppressWarnings({ "javadoc" })
    public <T> Optional<T> takeByID(final Object id,
                                    final Class<T> clazz) {
        return takeByID( id, clazz, WAIT_FOREVER_TIMEOUT );
    }

    // evict modifiers
    /**
     * the same as {@link #fetch(IBO, TransactionStatus, long, int, int)} with {@link JSpace#EVICT_ONLY} modifier and
     * <code>maxElements={@link Integer#MAX_VALUE}</code>. </p>
     * 
     * You can treat this method as delete from memory without deleting data from external data store. Also you can
     * specify timeout for evict operation.
     */
    @SuppressWarnings("javadoc")
    public Object[] evict(final Object template,
                          final int timeout) {
        return fetch( template, timeout, Integer.MAX_VALUE, JSpace.EVICT_ONLY );
    }

    /**
     * the same as {@link #fetch(IBO, TransactionStatus, long, int, int)} with {@link JSpace#EVICT_ONLY} modifier and
     * <code>timeout={@link #WAIT_FOREVER_TIMEOUT}</code>. </p>
     * 
     * @see #evict(IBO, long)
     */
    @SuppressWarnings("javadoc")
    public Object[] evict(final Object template) {
        return fetch( template, WAIT_FOREVER_TIMEOUT, Integer.MAX_VALUE, JSpace.EVICT_ONLY );
    }

    /**
     * the same as {@link #fetch(IBO, TransactionStatus, long, int, int)} with {@link JSpace#EVICT_ONLY} +
     * {@link JSpace#MATCH_BY_ID} modifier and <code>maxElements=1</code>. </p>
     */
    @SuppressWarnings({ "javadoc" })
    public <T> Optional<T> evictByID(final Object id,
                                     final Class<T> clazz,
                                     final int timeout) {
        final CacheStoreEntryWrapper wrapper = CacheStoreEntryWrapper.readByIdValueOf( delegate.getSpaceConfiguration().boFor( clazz ), id );
        final Object[] objects = fetch( wrapper, timeout, 1, JSpace.EVICT_ONLY | JSpace.MATCH_BY_ID );
        return SpaceUtility.singleResult( objects );
    }

    /**
     * the same as {@link #fetch(IBO, TransactionStatus, long, int, int)} with {@link JSpace#EVICT_ONLY} +
     * {@link JSpace#MATCH_BY_ID} modifier and <code>maxElements=1</code> where
     * <code>timeout={@link #WAIT_FOREVER_TIMEOUT}</code> </p>
     */
    @SuppressWarnings({ "javadoc" })
    public <T> Optional<T> evictByID(final Object id,
                                     final Class<T> clazz) {
        return evictByID( id, clazz, WAIT_FOREVER_TIMEOUT );
    }

    // pure delegation
    @Override
    public void notify(final Object template,
                       final SpaceNotificationListener listener,
                       final int modifiers) {
        delegate.notify( template, listener, modifiers );
    }

    @Override
    public Object[] fetch(final Object template,
                          final int timeout,
                          final int maxResults,
                          final int modifiers) {
        return delegate.fetch( template, timeout, maxResults, modifiers );
    }

    @Override
    public void write(final Object entry,
                      final int timeToLive,
                      final int timeout,
                      final int modifier) {
        delegate.write( entry, timeToLive, timeout, modifier );
    }

    @Override
    public long size() {
        return delegate.size();
    }

    @Override
    public int mbUsed() {
        return delegate.mbUsed();
    }

    @Override
    public SpaceTopology getSpaceTopology() {
        return delegate.getSpaceTopology();
    }

    @Override
    public void afterPropertiesSet()
                                    throws Exception {
        delegate.afterPropertiesSet();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    @Override
    public void destroy()
                         throws Exception {
        delegate.destroy();
    }

    @Override
    public void syncTx(final Object ctx,
                       final boolean commit) {
        delegate.syncTx( ctx, commit );
    }

    @Override
    public SpaceTransactionHolder getTransactionHolder() {
        return delegate.getTransactionHolder();
    }

    @Override
    public void bindTransactionHolder(final SpaceTransactionHolder transactionHolder) {
        delegate.bindTransactionHolder( transactionHolder );
    }

    @Override
    public Object unbindTransactionHolder(final SpaceTransactionHolder transactionHolder) {
        return delegate.unbindTransactionHolder( transactionHolder );
    }
}
