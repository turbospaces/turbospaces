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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Objects;
import com.turbospaces.api.SpaceException;
import com.turbospaces.spaces.EntryKeyLockQuard;
import com.turbospaces.spaces.KeyLocker;

/**
 * default key-lock manager implementation. this one uses write/read concurrent maps to store named locks and removes
 * lock objects only in case there is no threads(transactions) trying to acquire the same named lock. </p>
 * 
 * currently this class performs quite well, however uses shared synchronization lock which is a subject for revise
 * procedure.
 * 
 * @since 0.1
 */
@ThreadSafe
public final class TransactionScopeKeyLocker implements KeyLocker {
    private final Map<Object, LockMonitor> writeLocks = new HashMap<Object, LockMonitor>();

    @Override
    public LockMonitor writeLock(final Object key,
                                 final long transactionId,
                                 final long timeout,
                                 final boolean strict) {
        final Map<Object, LockMonitor> locks = writeLocks;
        LockMonitor monitor = null;

        synchronized ( locks ) {
            monitor = locks.get( key );
            if ( monitor == null ) {
                monitor = new LockMonitor( new TransactionModificationLock( strict ), key );
                locks.put( key, monitor );
            }
            monitor.acquires++;
        }

        try {
            boolean locked = timeout == Long.MAX_VALUE ? monitor.lock.lock( transactionId ) : monitor.lock.tryLock(
                    transactionId,
                    timeout,
                    TimeUnit.MILLISECONDS );
            return locked ? monitor : null;
        }
        catch ( InterruptedException e ) {
            Thread.currentThread().interrupt();
            throw new SpaceException( Thread.currentThread().toString() + " has been interrupted", e );
        }
    }

    @Override
    public void writeUnlock(final EntryKeyLockQuard keyGuard,
                            final long transactionId) {
        final Map<Object, LockMonitor> locks = writeLocks;
        final Object key = keyGuard.getKey();

        synchronized ( locks ) {
            LockMonitor monitor = locks.get( key );
            if ( monitor != null ) {
                monitor.lock.unlock( transactionId );
                if ( --monitor.acquires == 0 )
                    locks.remove( key );
            }
            else
                throw new IllegalStateException( String.format( "Illegal attempt to unlock key=%s under transaction = %s!", key, transactionId ) );
        }
    }

    private static final class LockMonitor implements EntryKeyLockQuard {
        private int acquires;
        private final TransactionModificationLock lock;
        private final Object key;

        private LockMonitor(final TransactionModificationLock lock, final Object key) {
            super();

            this.key = key;
            this.lock = lock;
        }

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper( this ).add( "key", getKey() ).toString();
        }

        @Override
        public int hashCode() {
            return getKey().hashCode();
        }

        @Override
        public boolean equals(final Object another) {
            if ( another instanceof LockMonitor )
                return getKey().equals( ( (LockMonitor) another ).getKey() );

            return super.equals( another );
        }
    }
}
