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
package com.turbospaces.pool;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

/**
 * High-performance concurrent(non-blocking) object pool factory implementation. This class does not support self
 * cleanup and if queue size grows queue objects will never expired. This can potentially cause some kind of memory
 * leaks, however working assumption is that queue size will never grow dramatically.
 * </p>
 * 
 * For convenience class provides {@link #clear()} method which removes all objects from pool.</p>
 * 
 * You must take into account that this object pool should not be used for all possible cases. Java6 introduced escape
 * analysis which can help to reduce amount of garbage created. It is recommended to use this pool only for heavy
 * objects or very frequently created objects.
 * 
 * @param <T>
 *            type of the object which is being re-used by this pool
 * 
 * @since 0.1
 */
@ThreadSafe
public class SimpleObjectPool<T> implements ObjectPool<T> {
    /**
     * elements counter is weekly consistent counter(not necessary strict as this is not very important)
     */
    private final AtomicInteger elementsCount = new AtomicInteger();
    private final ConcurrentLinkedQueue<T> queue = new ConcurrentLinkedQueue<T>();
    private final ObjectFactory<T> objFactory;
    private int maxElements = MAX_POOL_ELEMENTS;

    /**
     * created simple concurrent non-blocking object pool over object factory.
     * 
     * @param objFactory
     *            object instantiation factory
     */
    public SimpleObjectPool(final com.turbospaces.pool.ObjectFactory<T> objFactory) {
        super();
        this.objFactory = objFactory;
    }

    /**
     * change the default max pooled elements size
     * 
     * @param maxElements
     *            max re-used objects in pool
     */
    public void setMaxElements(final int maxElements) {
        this.maxElements = maxElements;
    }

    @Override
    public T borrowObject() {
        T t = queue.poll();
        if ( t == null )
            t = objFactory.newInstance();
        else {
            elementsCount.decrementAndGet();
            if ( t instanceof Reusable )
                ( (Reusable) t ).reset();
        }
        return t;
    }

    @Override
    public void returnObject(final T obj) {
        if ( obj != null ) {
            objFactory.invalidate( obj );
            // weakly consistent
            if ( elementsCount.get() < maxElements ) {
                queue.offer( obj );
                elementsCount.incrementAndGet();
            }
        }
    }

    /**
     * remove all cached objects from pool. this method can be exposed through <strong>JMX</strong> for convenience.
     */
    public void clear() {
        queue.clear();
    }
}
