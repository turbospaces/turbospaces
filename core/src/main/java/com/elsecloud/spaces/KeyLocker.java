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
package com.elsecloud.spaces;

/**
 * Read-write locker that adds ACID behavior to the jspace topology(do not allow concurrent modification by the same
 * key). Provides method for acquiring both exclusiveRead/write locks.
 * 
 * @since 0.1
 */
public interface KeyLocker {

    /**
     * try to acquire write lock for particular key within given timeout clause.
     * 
     * @param key
     *            primary key that needs to be locked for write
     * @param transactionID
     *            transaction identifier
     * @param timeout
     *            max amount of time for write-lock acquire
     * @param strictMode
     *            whether strict validation should be performed for exclusively held thread and current thread
     * @return non-null entry lock if lock has been acquired otherwise <code>null</code>
     */
    EntryKeyLockQuard writeLock(Object key,
                                long transactionID,
                                long timeout,
                                boolean strictMode);

    /**
     * unlock write lock for particular key.
     * 
     * @param key
     *            map key that needs to be unlocked for write
     * @param transactionID
     *            transaction identifier
     */
    void writeUnlock(EntryKeyLockQuard key,
                     long transactionID);
}
