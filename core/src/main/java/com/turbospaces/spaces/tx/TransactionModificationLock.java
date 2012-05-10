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
package com.turbospaces.spaces.tx;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedLongSynchronizer;

import javax.annotation.concurrent.Immutable;

import org.springframework.util.ObjectUtils;

/**
 * Re-reentrant exclusion lock class that uses the value zero to represent the unlocked state, and long to represent the
 * locked state (this long value is actually transaction modification identifier).
 * 
 * @since 0.1
 */
@Immutable
class TransactionModificationLock {
    private final Sync sync;

    TransactionModificationLock(final boolean exclusiveMode) {
        sync = new Sync( exclusiveMode );
    }

    /**
     * synchronization control for the transaction modification lock.
     */
    @SuppressWarnings("serial")
    private static final class Sync extends AbstractQueuedLongSynchronizer {
        private final boolean exclusiveMode;

        private Sync(final boolean exclusiveMode) {
            super();
            this.exclusiveMode = exclusiveMode;
        }

        @Override
        protected boolean tryAcquire(final long transactionId) {
            Thread current = Thread.currentThread();
            long c = getState();
            boolean success = false;

            if ( c == 0 ) {
                if ( compareAndSetState( 0, transactionId ) ) {
                    if ( exclusiveMode )
                        setExclusiveOwnerThread( current );
                    success = true;
                }
            }
            else if ( transactionId == c ) {
                success = true;
                if ( exclusiveMode )
                    success = current == getExclusiveOwnerThread();
            }
            return success;
        }

        @Override
        protected boolean tryRelease(final long transactionId) {
            long c = getState();
            if ( exclusiveMode && Thread.currentThread() != getExclusiveOwnerThread() )
                throw new IllegalMonitorStateException( String.format(
                        "Current Thread = %s is not owner of the lock, lock is held by = %s",
                        Thread.currentThread(),
                        ObjectUtils.nullSafeToString( getExclusiveOwnerThread() ) ) );
            boolean free = false;
            if ( c == 0 || c == transactionId ) {
                free = true;
                setExclusiveOwnerThread( null );
                setState( 0 );
            }

            return free;
        }
    }

    boolean lock(final long transactionID) {
        sync.acquire( transactionID );
        return true;
    }

    boolean tryLock(final long transactionID,
                    final long timeout,
                    final TimeUnit unit)
                                        throws InterruptedException {
        return sync.tryAcquireNanos( transactionID, unit.toNanos( timeout ) );
    }

    void unlock(final long transactionID) {
        sync.release( transactionID );
    }
}
