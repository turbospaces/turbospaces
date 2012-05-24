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

/**
 * this is configuration class which allows you to control the capacity off-heap data structure(both max elements and
 * megabytes).</p>
 * 
 * @since 0.1
 */
public class CapacityRestriction {
    private long maxMemorySizeInBytes = Long.MAX_VALUE;
    private long maxElements = Integer.MAX_VALUE / 16;

    /**
     * @return the maximum memory size in megabytes (default value {@code Integer.MAX_VALUE / 16}).
     */
    public long getMaxMemorySizeInBytes() {
        return maxMemorySizeInBytes;
    }

    /**
     * set the maximum megabytes that can be used by off-heap data structures.
     * 
     * @param maxMemorySize
     *            max memory that can be used by off-heap data structure in bytes
     */
    public void setMaxMemorySizeInMb(final long maxMemorySize) {
        Preconditions.checkArgument( maxMemorySize > 0, " maxMemorySize must be positive" );
        this.maxMemorySizeInBytes = com.turbospaces.core.Memory.mb( maxMemorySize );
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
}
