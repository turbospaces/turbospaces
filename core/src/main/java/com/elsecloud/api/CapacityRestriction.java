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
package com.elsecloud.api;

import com.google.common.base.Preconditions;

/**
 * this is configuration class which allows you to control the capacity of the jspace(both the maximum elements and
 * maximum megabytes ) on both jspace level and entity level.</p>
 * 
 * by default capacity is not restricted and it is assumed that the configuration can be changed in runtime
 * 
 * @since 0.1
 */
public class CapacityRestriction {
    private volatile int maxMemorySizeInMb = Integer.MAX_VALUE;
    private volatile long maxElements = Integer.MAX_VALUE;

    /**
     * @return the maximum memory size in megabytes
     */
    public int getMaxMemorySizeInMb() {
        return maxMemorySizeInMb;
    }

    /**
     * set the maximum megabytes that can be used by jspace to store space entities(on class level or just space
     * level)
     * 
     * @param maxMemorySizeInMb
     */
    public void setMaxMemorySizeInMb(final int maxMemorySizeInMb) {
        Preconditions.checkArgument( maxMemorySizeInMb > 0, " maxMemory must be positive" );
        this.maxMemorySizeInMb = maxMemorySizeInMb;
    }

    /**
     * @return the maximum elements (space capacity)
     */
    public long getMaxElements() {
        return maxElements;
    }

    /**
     * set the maximum elements that can be stored in jspace(on class level or just space
     * level)
     * 
     * @param maxElements
     */
    public void setMaxElements(final long maxElements) {
        Preconditions.checkArgument( maxElements > 0, "maxElements must be positive" );
        this.maxElements = maxElements;
    }
}
