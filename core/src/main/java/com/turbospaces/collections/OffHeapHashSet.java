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
import java.util.Collection;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import com.turbospaces.offmemory.ByteArrayPointer;

/**
 * off-heap map abstraction(if fact dramatic simplification - even classical {@link Collection#size()} method is not
 * present here). Concrete implementations could use linear probing or linked lists for collision resolutions or any
 * other king of collision hashing algorithms. </p>
 * 
 * <b>NOTE:</b> Concrete implementation must be thread-safe generally (generally means that the thread-safety
 * restrictions is not strict because there is always high-level coordinator (which guarantees ACID behavior in case of
 * concurrent modifications by same primary key) so that it is not possible that different threads modifying this set
 * concurrently by the same key). remove/put operations can't happen in parallel for the same key, however just read in
 * never blocking operation.
 * 
 * @since 0.1
 */
public interface OffHeapHashSet extends DisposableBean, InitializingBean {
    /**
     * max set capacity currently supported by off-heap hash set implementations
     */
    int MAXIMUM_CAPACITY = 1 << 30;

    /**
     * default initial capacity of the set of <code>1024</code>.
     */
    int DEFAULT_INITIAL_CAPACITY = 1 << 12;

    /**
     * check whether particular key is present in map and is so, return the off-heap pointer address.</p>
     * 
     * <b>NOTE:</b>concrete implementation <b>must</b> remove expired entities automatically in case of lease timeout
     * 
     * @param key
     *            primary key
     * @return the pointer's address if key exists or <code>0</code> if not
     */
    boolean contains(Object key);

    /**
     * get the byte array pointer associated with the given key.</p>
     * 
     * <b>NOTE:</b>concrete implementation <b>must</b> remove expired entities automatically in case of lease timeout
     * 
     * @param key
     *            primary key
     * @return byte array pointer associated with key
     */
    ByteArrayPointer getAsPointer(Object key);

    /**
     * get the serialized data associated with the given key. </p>
     * 
     * <b>NOTE:</b>concrete implementation <b>must</b> remove expired entities automatically in case of lease timeout
     * 
     * @param key
     * @return byte array
     */
    ByteBuffer getAsSerializedData(Object key);

    /**
     * put(and probably replace previous pointer associated with the key(and do memory utilization in case of such
     * previous entity existence)) byte array pointer and associate it with given key.
     * 
     * @param key
     *            primary key
     * @param value
     *            byte array pointer
     * @return how many bytes where occupied by previous byte array pointer or zero if none.
     */
    int put(Object key,
            ByteArrayPointer value);

    /**
     * remove byte array pointer associated with given key(and perform memory utilization in case of key existence).
     * 
     * @param key
     *            primary key
     * @return how many bytes where occupied by byte array pointer
     */
    int remove(Object key);
}
