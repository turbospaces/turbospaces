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
package com.turbospaces;

import java.nio.ByteBuffer;

import com.google.common.cache.Cache;
import com.turbospaces.offmemory.ByteArrayPointer;

/**
 * The root interface in the <i>turbospaces collections</i> hierarchy. Actually this is more internal interface rather
 * than external because it is really low-level and generally users will use implementation of {@link Cache} interface
 * or JDK standard interfaces. The main purpose for definition interface as separate unit is abstraction that is
 * suitable for both JDK collections and guava cache and more important turbospace's java spaces specific
 * implementation.</p>
 * 
 * <b>NOTE :</b> particular implementation of this interface uses off-heap memory space(dramatically reduces the java
 * heap space and GC factor). Currently the only one supported mechanism for such off-heap allocation is
 * {@link sun.misc.Unsafe#allocateMemory(long)} and {@link sun.misc.Unsafe#reallocateMemory(long, long)} methods, but
 * this is subject for modifications in future(off-heap store can be mapped buffer files over SSD disks for
 * example).</p>
 * 
 * Most method as self-explained so there is no need for detailed documentation as such.
 * 
 * @since 0.1
 */
@SuppressWarnings("restriction")
public interface OffHeapHashSet {
    /**
     * check whether particular value (associated with given key) is present in this collection.</p>
     * 
     * <b>NOTE : </b>concrete implementation <b>must</b> remove expired entities automatically during read if such
     * expiration has been detected.
     * 
     * @param key
     *            primary key
     * @return <tt>true</tt> if this collection contains the specified element
     */
    boolean contains(Object key);

    /**
     * get the byte array pointer associated with the given key.</p>
     * 
     * <b>NOTE:</b>concrete implementation <b>must</b> remove expired entities automatically in case of lease expiration
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

    /**
     * similar to spring's {@code DisposabelBean} - release off-heap resources.
     */
    void destroy();
}