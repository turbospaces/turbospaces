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
package com.elsecloud.pool;

import com.elsecloud.offmemory.ByteArrayPointer;
import com.elsecloud.spaces.tx.TransactionModificationContext;

/**
 * High-performance concurrent object pool factory. Typically there are number of cases when you would
 * like to re-use objects instead of re-creating because this can be expensive. This class allows you to borrow object
 * from pool and after usage return object back. </p>
 * 
 * In-fact this class does not rely on apache-commons-pool for simplicity purposes and for performance reasons. </p>
 * 
 * <b>USAGE assumptions:</b> objectFactory to be embedded into reusable class and reusable class will hide default
 * constructor making outer initialization impossible. See {@link ByteArrayPointer} and
 * {@link TransactionModificationContext} for example.
 * 
 * @since 0.1
 * @param <T>
 *            type of the object which is being re-used by this pool
 */
public interface ObjectPool<T> {
    /**
     * max element to cache
     */
    int MAX_POOL_ELEMENTS = 1 << 12;

    /**
     * borrow object from pool. If no free objects available, create new one using object factory and return.
     * Never blocks or throws some kind of capacity violation exceptions, instead can use weak references in order to
     * automatically cleanup resources if necessary.
     * 
     * @return object that is being re-used or new if no free objects available.
     */
    public abstract T borrowObject();

    /**
     * return object to the pool. ObjectFactory is responsible for invalidating the state of the object.
     * 
     * @param obj
     */
    public abstract void returnObject(final T obj);
}
