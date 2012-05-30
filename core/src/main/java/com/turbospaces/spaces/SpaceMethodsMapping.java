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

import com.turbospaces.api.JSpace;
import com.turbospaces.offmemory.OffHeapCacheStore;
import com.turbospaces.spaces.tx.SpaceTransactionManager;

/**
 * RPC jgroups method mapping.
 * 
 * @since 0.1
 * @see OffHeapCacheStore
 * @see RemoteJSpace
 * @see SpaceTransactionManager
 */
public enum SpaceMethodsMapping {
    /**
     * remote write method identifier.
     * 
     * @see JSpace#write(Object, int, int, int)
     */
    WRITE,
    /**
     * remote fetch method identifier
     * 
     * @see JSpace#fetch(Object, int, int, int)
     */
    FETCH,
    /**
     * remote notify method identifier
     * 
     * @see JSpace#notify(Object, com.turbospaces.api.SpaceNotificationListener, int)
     */
    NOTIFY,
    /**
     * remote size method identifier
     * 
     * @see JSpace#size()
     */
    SIZE,
    /**
     * remote size method identifier
     * 
     * @see JSpace#mbUsed()
     */
    MB_USED,
    /**
     * remote evict all method identifier
     * 
     * @see JSpace#evictAll()
     */
    EVICT_ALL,
    /**
     * remote evictPercantage method identifier.
     * 
     * @see JSpace#evictPercentage(int)
     */
    EVICT_PERCENTAGE,
    /**
     * remote evictElements method identifier.
     * 
     * @see JSpace#evictElements(long)
     */
    EVICT_ELEMENTS,
    /**
     * remote space topology method identifier
     * 
     * @see JSpace#getSpaceTopology()
     */
    SPACE_TOPOLOGY,
    /**
     * start remote space transaction method identifier
     * 
     * @see SpaceTransactionManager#getTransaction(org.springframework.transaction.TransactionDefinition)
     */
    BEGIN_TRANSACTION,
    /**
     * commit remote space transaction method identifier
     * 
     * @see SpaceTransactionManager#commit(org.springframework.transaction.TransactionStatus)
     */
    COMMIT_TRANSACTION,
    /**
     * rollback remote space transaction method identifier
     * 
     * @see SpaceTransactionManager#rollback(org.springframework.transaction.TransactionStatus)
     */
    ROLLBACK_TRANSACTION;
}
