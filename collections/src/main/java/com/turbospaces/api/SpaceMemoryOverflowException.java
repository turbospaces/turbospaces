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

import java.util.Arrays;

/**
 * signals that cache memory exceeded it's max capacity and it is not possible to add more elements.
 * typically you would like to restrict max off-heap memory buffer because uncontrolled growth can lead to
 * unmaintainable state of operation system.
 * 
 * @since 0.1
 * @see CapacityRestriction
 */
public class SpaceMemoryOverflowException extends RuntimeException {
    private static final long serialVersionUID = -4537158624229189977L;

    private final long bytes;
    private final byte[] serializedState;

    /**
     * create new space memory overflow exception and specify space's max memory capacity and object's state that
     * attempted to be added to the
     * space.
     * 
     * @param bytes
     *            max megabytes of off-heap memory used by space in bytes
     * @param serializedState
     *            bytes representation of object
     */
    public SpaceMemoryOverflowException(final long bytes, final byte[] serializedState) {
        super( String.format( "Space exceeded it's memory capacity %s bytes, attempt to add %s rejected.", bytes, Arrays.toString( serializedState ) ) );
        this.bytes = bytes;
        this.serializedState = Arrays.copyOf( serializedState, serializedState.length );
    }

    /**
     * @return maximum megabytes that can be used by space (off-heap cache store).
     */
    public long getBytes() {
        return bytes;
    }

    /**
     * @return bytes array which failed to be added to the heap.
     */
    public byte[] getSerializedState() {
        return serializedState;
    }
}
