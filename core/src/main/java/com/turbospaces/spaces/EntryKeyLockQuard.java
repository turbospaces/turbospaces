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

/**
 * protects(guards) space entry(row) from concurrent modification. this is something that needs to be associated with
 * primary key of the jspace's entry.</p>
 * 
 * Such kind of entry lock guard can be used with <code>synchronized</code> block in order to protect safe concurrent
 * modifications. </p>
 * 
 * <b>NOTE:</b> for performance reasons you can compare {@link EntryKeyLockQuard} by reference instead of
 * {@link Object#equals(Object)} method because you can expect that {@link KeyLocker} will return the same instance of
 * {@link EntryKeyLockQuard} for the same key, however you should bear in mind that
 * {@link KeyLocker#writeLock(Object, long, long, boolean)} will definitely return different instances.
 * 
 * @since 0.1
 * @see KeyLocker
 */
public interface EntryKeyLockQuard {

    /**
     * @return unique key of entity for each this lock has been acquired.
     */
    Object getKey();

    /**
     * @return hash code of the key
     */
    @Override
    abstract int hashCode();

    /**
     * check whether underlying key equals another given key.
     * 
     * @param another
     * @return true if equals by underlying keys, else false.
     */
    @Override
    abstract boolean equals(Object another);
}
