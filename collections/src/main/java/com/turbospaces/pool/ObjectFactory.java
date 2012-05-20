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

/**
 * object factory responsible for creating new objects and recycling.
 * 
 * @param <T>
 *            type of produced objects
 * 
 * @since 0.1
 */
public interface ObjectFactory<T> {

    /**
     * create new instance of object via <code>new</code> keyword.
     * 
     * @param args
     * @return new instance of the object using new keyword.
     */
    T newInstance();

    /**
     * invalidate(recycle) the state of the object, restore to the initial state(clean bean properties, etc).
     * 
     * @param obj
     */
    void invalidate(T obj);
}
