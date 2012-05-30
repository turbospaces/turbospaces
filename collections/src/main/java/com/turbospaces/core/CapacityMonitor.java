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
package com.turbospaces.core;

import com.lmax.disruptor.Sequence;
import com.turbospaces.api.CapacityRestriction;
import com.turbospaces.api.SpaceCapacityOverflowException;
import com.turbospaces.api.SpaceMemoryOverflowException;
import com.turbospaces.offmemory.ByteArrayPointer;

/**
 * thread-safe space capacity restriction holder.
 * 
 * @since 0.1
 */
public final class CapacityMonitor {
    private final Sequence memoryUsed = new Sequence( 0 );
    private final Sequence itemsCount = new Sequence( 0 );
    private final CapacityRestriction capacityRestriction;
    private final CapacityMonitor globalCapacityMonitor;

    /**
     * create new capacity monitor for given capacityRestriction configuration and associate globalCapacityRestrion
     * 
     * @param capacityRestriction
     *            capacity restriction on level class
     * @param globalCapacityRestriction
     *            global space capacity restriction
     */
    public CapacityMonitor(final CapacityRestriction capacityRestriction, final CapacityRestriction globalCapacityRestriction) {
        super();
        this.capacityRestriction = capacityRestriction;
        this.globalCapacityMonitor = globalCapacityRestriction != null ? new CapacityMonitor( globalCapacityRestriction, null ) : null;
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
     * capacity restriction configuration associated with this monitor.</p>
     * 
     * @return capacity restriction configuration
     */
    public CapacityRestriction getCapacityRestriction() {
        return capacityRestriction;
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
        //
        memoryUsed.addAndGet( addedBytes );
        if ( globalCapacityMonitor != null )
            globalCapacityMonitor.add( addedBytes, prevBytesOccupation );
    }

    /**
     * modify the current capacity restriction by the value of removedBytes field
     * 
     * @param removedBytes
     *            how many bytes where removed
     */
    public void remove(final int removedBytes) {
        if ( removedBytes > 0 ) {
            long currentMemoryUsed = memoryUsed.addAndGet( -removedBytes );
            long currentItemsCount = itemsCount.addAndGet( -1 );
            assert currentItemsCount >= 0;
            assert currentMemoryUsed >= 0;
        }

        if ( globalCapacityMonitor != null )
            globalCapacityMonitor.remove( removedBytes );
    }

    /**
     * ensure enough capacity for new entity and throw capacity overflow exception if necessary.
     * 
     * @param pointer
     *            new byte array pointer
     * @param obj
     *            the actual entity that needs to be added to the space
     * 
     * @throws SpaceCapacityOverflowException
     *             in case of capacity overflow
     * @throws SpaceMemoryOverflowException
     *             in case of memory overflow
     */
    public void ensureCapacity(final ByteArrayPointer pointer,
                               final Object obj) {
        if ( globalCapacityMonitor != null ) {
            JVMUtil.ensureEnoughMemoryCapacity( pointer, globalCapacityMonitor.capacityRestriction, globalCapacityMonitor.memoryUsed );
            JVMUtil.ensureEnoughCapacity( obj, globalCapacityMonitor.capacityRestriction, globalCapacityMonitor.itemsCount );
        }

        JVMUtil.ensureEnoughMemoryCapacity( pointer, capacityRestriction, memoryUsed );
        JVMUtil.ensureEnoughCapacity( obj, capacityRestriction, itemsCount );
    }
}
