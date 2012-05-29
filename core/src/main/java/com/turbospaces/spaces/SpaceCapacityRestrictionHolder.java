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

import com.lmax.disruptor.Sequence;
import com.turbospaces.api.CapacityRestriction;
import com.turbospaces.core.SpaceUtility;
import com.turbospaces.offmemory.ByteArrayPointer;

/**
 * thread-safe space capacity restriction holder.
 * 
 * @since 0.1
 */
public class SpaceCapacityRestrictionHolder {
    private final Sequence memoryUsed = new Sequence( 0 );
    private final Sequence itemsCount = new Sequence( 0 );
    private final CapacityRestriction capacityRestriction;

    @SuppressWarnings("javadoc")
    public SpaceCapacityRestrictionHolder(final CapacityRestriction capacityRestriction) {
        super();
        this.capacityRestriction = capacityRestriction;
    }

    /**
     * @return currently used memory in bytes
     */
    public long getMemoryUsed() {
        return memoryUsed.get();
    }

    /**
     * @return elements count
     */
    public long getItemsCount() {
        return itemsCount.get();
    }

    /**
     * modify the current capacity restriction by the value of addedBytes field (and optional prevOccupation)
     * 
     * @param addedBytes
     *            how many bytes were added
     * @param prevBytesOccupation
     *            how many bytes where by previous entry
     */
    public void add(final int addedBytes,
                    final int prevBytesOccupation) {
        if ( prevBytesOccupation > 0 )
            memoryUsed.addAndGet( -prevBytesOccupation );
        else
            itemsCount.incrementAndGet();
        memoryUsed.addAndGet( addedBytes );
    }

    /**
     * modify the current capacity restriction by the value of removedBytes field
     * 
     * @param removedBytes
     *            how many bytes where removed
     */
    public void remove(final int removedBytes) {
        if ( removedBytes > 0 ) {
            memoryUsed.addAndGet( -removedBytes );
            itemsCount.addAndGet( -1 );
        }
    }

    /**
     * ensure enough capacity for new entity
     * 
     * @param pointer
     *            new byte array pointer
     * @param obj
     *            the actual entity that needs to be added to the space
     */
    public void ensureCapacity(final ByteArrayPointer pointer,
                               final Object obj) {
        SpaceUtility.ensureEnoughMemoryCapacity( pointer, capacityRestriction, memoryUsed );
        SpaceUtility.ensureEnoughCapacity( obj, capacityRestriction, itemsCount );
    }
}
