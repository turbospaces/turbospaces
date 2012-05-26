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
package com.turbospaces.api;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * this is configuration class which allows you to control the capacity off-heap data structure(both max elements and
 * megabytes) and also specify eviction configuration for space overflow situations.</p>
 * 
 * by default maxElements configured to be {@code Integer.MAX_VALUE / 16} and the maxMemorySize is not restricted,
 * default eviction policy is {@link CacheEvictionPolicy#REJECT}.
 * 
 * @since 0.1
 */
public final class CapacityRestriction implements Cloneable {
    private long maxMemorySizeInBytes = Long.MAX_VALUE;
    private long maxElements = Integer.MAX_VALUE / 16;
    private CacheEvictionPolicy evictionPolicy = CacheEvictionPolicy.REJECT;
    private int evictionPercentage = 10;

    /**
     * @return the maximum memory size in megabytes (default value {@code Integer.MAX_VALUE / 16}).
     */
    public long getMaxMemorySizeInBytes() {
        return maxMemorySizeInBytes;
    }

    /**
     * set the maximum megabytes that can be used by off-heap data structures.
     * 
     * @param mb
     *            max memory that can be used by off-heap data structure in megabytes
     */
    public void setMaxMemorySizeInMb(final long mb) {
        Preconditions.checkArgument( mb > 0, " maxMemorySize must be positive" );
        this.maxMemorySizeInBytes = com.turbospaces.core.Memory.mb( mb );
    }

    /**
     * @return the maximum elements (capacity)
     */
    public long getMaxElements() {
        return maxElements;
    }

    /**
     * set the maximum elements that can be stored in off-heap memory data structures
     * 
     * @param maxElements
     *            maximum capacity
     */
    public void setMaxElements(final long maxElements) {
        Preconditions.checkArgument( maxElements > 0, "maxElements must be positive" );
        this.maxElements = maxElements;
    }

    /**
     * @return associated eviction policy.
     */
    public CacheEvictionPolicy getEvictionPolicy() {
        return evictionPolicy;
    }

    /**
     * set the cache eviction policy
     * 
     * @param evictionPolicy
     *            new cache eviction policy
     */
    public void setEvictionPolicy(final CacheEvictionPolicy evictionPolicy) {
        this.evictionPolicy = evictionPolicy;
    }

    /**
     * @return the eviction percentage between 0..100
     */
    public int getEvictionPercentage() {
        return evictionPercentage;
    }

    /**
     * specify the cache eviction percentage. you would need to set {@link #setEvictionPolicy(CacheEvictionPolicy)} LRU
     * eviction policy first.</p>
     * 
     * default eviction percentage is 10%.
     * 
     * @param evictionPercentage
     *            value between 0..100
     */
    public void setEvictionPercentage(final int evictionPercentage) {
        Preconditions.checkState( getEvictionPolicy().isEviction(), "configure eviction policy first" );
        Preconditions.checkArgument( evictionPercentage > 0, "eviction percentage must be positive" );
        Preconditions.checkArgument( evictionPercentage < 100, "eviction percentage must be < 100%" );

        this.evictionPercentage = evictionPercentage;
    }

    @Override
    public CapacityRestriction clone() {
        for ( ;; )
            try {
                return (CapacityRestriction) super.clone();
            }
            catch ( CloneNotSupportedException e ) {
                Throwables.propagate( e );
            }
    }
}
