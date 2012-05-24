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

import java.nio.ByteBuffer;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import com.turbospaces.api.JSpace;
import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.core.CacheStatisticsCounter;
import com.turbospaces.model.CacheStoreEntryWrapper;
import com.turbospaces.offmemory.IndexManager;
import com.turbospaces.spaces.tx.TransactionModificationContext;

/**
 * This is the central interface which is responsible for high-level jspace interactions orchestration (manage
 * write/take/read operations), guard of ACID jspace behavior via built-in synchronization mechanisms, protect jspace
 * from illegal concurrent modifications from parallel transactions. Another very important thing is managing of
 * off-heap operations, buffering direct byte array allocation which can exceed JVM memory
 * restrictions and allows to manipulate data outside garbage collector without GC pauses.</p>
 * 
 * @since 0.1
 */
public interface SpaceStore extends DisposableBean, InitializingBean {

    /**
     * synchronize(apply all or discard) transaction modification changes and release write/read locks
     * held by transaction modification context.
     * 
     * @param modificationContext
     * @param apply
     *            sync or discard all changes
     */
    void sync(TransactionModificationContext modificationContext,
              boolean apply);

    /**
     * refer to JSpace#write(IBO, long, long, int) documentation. The only one difference is that instead of writing
     * everything to store, you need to register changes within transaction modification context.
     * 
     * @see JSpace#WRITE_ONLY
     * @see JSpace#UPDATE_ONLY
     * @see JSpace#WRITE_OR_UPDATE
     */
    @SuppressWarnings({ "javadoc" })
    void write(CacheStoreEntryWrapper entry,
               TransactionModificationContext modificationContext,
               int timeToLive,
               int timeout,
               int modifier);

    /**
     * refer to {@link JSpace#fetch(IBO, long, int, int)} documentation. The only one difference is that instead of
     * writing everything to store, you need to register changes within transaction modification context.</p>
     * 
     * also the entities itself returned as {@link ByteBuffer} and the higher level abstraction handle transformation to
     * real objects if necessary.
     * 
     * @see JSpace#READ_ONLY
     * @see JSpace#TAKE_ONLY
     * @see JSpace#EVICT_ONLY
     * @see JSpace#MATCH_BY_ID
     * @see JSpace#EXCLUSIVE_READ_LOCK
     */
    @SuppressWarnings({ "javadoc" })
    ByteBuffer[] fetch(CacheStoreEntryWrapper template,
                       TransactionModificationContext modificationContext,
                       int timeout,
                       int maxResults,
                       int modifiers);

    /**
     * @return the index manager associated with this space store container.
     */
    IndexManager getIndexManager();

    /**
     * @return jspace configuration associated with jspace
     */
    SpaceConfiguration getSpaceConfiguration();

    /**
     * snapshot cache access statistics
     * 
     * @return snapshot with performance statistics
     */
    CacheStatisticsCounter.CompleteCacheStats stats();
}
