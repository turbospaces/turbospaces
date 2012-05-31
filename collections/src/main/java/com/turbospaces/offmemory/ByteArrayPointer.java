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
package com.turbospaces.offmemory;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.Immutable;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.turbospaces.core.EffectiveMemoryManager;

/**
 * Off-heap memory pointer reference. This is low-level proxy over off-heap address.
 * 
 * @since 0.1
 */
@Immutable
public final class ByteArrayPointer {

    /**
     * Entities are stored in special internal format (in fact format is quite trivial).</p>
     * 
     * <h3>Format is:</h3>
     * <ul>
     * <li>length - int(how many bytes are being occupied)</li>
     * <li>creationTimestamp - long(when the entry has been been added in milliseconds)</li>
     * <li>timeToLive - long(how long to live after initial write)</li>
     * <li>hitsCount - long(how many hits)</li>
     * <li>data - actual entitie's state in de-serialized format</li>
     * </ul>
     * 
     * @since 0.1
     */
    private static enum FormatFields {
        LENGTH(Ints.BYTES),
        CREATION_TIMESTAMP(Longs.BYTES),
        TIME_TO_LIVE(Ints.BYTES),
        LAST_ACCESS_DATE(Longs.BYTES),
        DATA(Integer.MAX_VALUE);

        private FormatFields(final int lenght) {
            this.lenght = lenght;
        }

        private final int lenght;
        private int offset;
    }

    private static final int INTERNAL_BYTES_OCCUPATION;

    static {
        int offset = 0;
        for ( FormatFields f : FormatFields.values() ) {
            f.offset = offset;
            offset += f.lenght;
        }
        INTERNAL_BYTES_OCCUPATION = FormatFields.LENGTH.lenght + FormatFields.CREATION_TIMESTAMP.lenght + FormatFields.TIME_TO_LIVE.lenght
                + FormatFields.LAST_ACCESS_DATE.lenght;
    }

    private final EffectiveMemoryManager memoryManager;
    private byte[] serializedBytes;
    private ByteBuffer serializedData;
    private long address;
    private Object object;
    private int ttl;

    /**
     * create new byte array pointer at given address and byte array(buffer) - this is constructor is used for reading
     * from off-heap memory.
     * 
     * @param memoryManager
     *            off-heap memory manager
     * @param address
     *            off-heap memory address
     * @param serializedData
     *            actual de-serialized state of the entity(entry)
     */
    public ByteArrayPointer(final EffectiveMemoryManager memoryManager, final long address, final ByteBuffer serializedData) {
        assert serializedData != null;

        this.memoryManager = memoryManager;
        this.address = address;
        this.serializedData = serializedData;
    }

    /**
     * create new byte array pointer for the given de-serialized entry and associate original entry(as reference) with
     * this pointer. Also time-to-live must be provided.
     * 
     * @param memoryManager
     *            off-heap memory manager
     * @param serializedData
     *            serialized entiti'es state
     * @param object
     *            target object (this {@link ByteArrayPointer} created over object)
     * @param ttl
     *            time-to-live
     */
    public ByteArrayPointer(final EffectiveMemoryManager memoryManager, final byte[] serializedData, final Object object, final int ttl) {
        assert serializedData != null;

        this.memoryManager = memoryManager;
        this.object = object;
        this.serializedBytes = serializedData;
        this.ttl = ttl;
    }

    /**
     * read the latest access timestamp for this pointer.</p>
     * 
     * <b>NOTE:</b> this method potentially not-thread safe and you can see stale data, so please make sure to call this
     * under proper lock.
     * 
     * @param address
     *            off-heap memory address
     * @param offHeapMmoryManager
     *            off-heap memory manager
     * @return the last access timestamp for this pointer
     */
    public static long getLastAccessTime(final long address,
                                         final EffectiveMemoryManager offHeapMmoryManager) {
        return offHeapMmoryManager.getLong( address + FormatFields.LAST_ACCESS_DATE.offset );
    }

    /**
     * update the last access timestamp to given value.
     * 
     * @param offHeapMmoryManager
     *            off-heap memory manager
     * @param address
     *            off-heap memory address
     * @param now
     *            new last access timestamp value
     */
    public static void updateLastAccessTime(final long address,
                                            final long now,
                                            final EffectiveMemoryManager offHeapMmoryManager) {
        offHeapMmoryManager.putLong( address + FormatFields.LAST_ACCESS_DATE.offset, now );
    }

    /**
     * read actual state of entity (without meta information) at the given address.</p>
     * 
     * @param address
     *            off-heap memory address
     * @param offHeapMmoryManager
     *            off-heap memory manager
     * @return buffer over internal state
     */
    public static byte[] getEntityState(final long address,
                                        final EffectiveMemoryManager offHeapMmoryManager) {
        return offHeapMmoryManager.readBytesArray(
                address + FormatFields.DATA.offset,
                offHeapMmoryManager.getInt( address + FormatFields.LENGTH.offset ) );
    }

    /**
     * read how many bytes are being occupied by underlying de-serialized entry's data.
     * 
     * @param offHeapMmoryManager
     *            off-heap memory manager
     * @param address
     *            off-heap memory address
     * @return how many bytes occupied by pointer
     */
    public static int getBytesOccupied(final long address,
                                       final EffectiveMemoryManager offHeapMmoryManager) {
        return offHeapMmoryManager.getInt( address + FormatFields.LENGTH.offset ) + INTERNAL_BYTES_OCCUPATION;
    }

    /**
     * read the creation timestamp of the associated space store entry(when the entry has been added).
     * 
     * @param address
     *            off-heap memory address
     * @param offHeapMmoryManager
     *            off-heap memory manager
     * @return creation timestamp
     */
    public static long getCreationTimestamp(final long address,
                                            final EffectiveMemoryManager offHeapMmoryManager) {
        return offHeapMmoryManager.getLong( address + FormatFields.CREATION_TIMESTAMP.offset );
    }

    /**
     * get the time-to-live for associated space store entity(how long to keep entry living)
     * 
     * @param address
     *            off-heap memory address
     * @param offHeapMemoryManager
     *            off-heap memory manager
     * @return ttl time-to-live associate with entry
     */
    public static int getTimeToLive(final long address,
                                    final EffectiveMemoryManager offHeapMemoryManager) {
        return offHeapMemoryManager.getInt( address + FormatFields.TIME_TO_LIVE.offset );
    }

    /**
     * get (read if necessary) the state from the off-heap memory
     * 
     * @return byte array representation of the object
     */
    public byte[] getSerializedData() {
        if ( serializedBytes != null )
            return serializedBytes;
        return serializedData.array();
    }

    /**
     * get the state from the off-heap memory as byte buffer
     * 
     * @return byte buffer
     */
    public ByteBuffer getSerializedDataBuffer() {
        if ( serializedData != null )
            return serializedData;
        return ByteBuffer.wrap( serializedBytes );
    }

    /**
     * utilized(dispose) used resources.
     */
    public void utilize() {
        if ( address != 0 )
            memoryManager.freeMemory( address );
        address = 0;
    }

    /**
     * @return how many off-heap bytes are being occupied by this pointer
     */
    public int bytesOccupied() {
        return getSerializedData().length + INTERNAL_BYTES_OCCUPATION;
    }

    /**
     * @return true if pointer's data is expired
     */
    public boolean isExpired() {
        return isExpired( getAddress(), memoryManager );
    }

    /**
     * @return object for which this {@link ByteArrayPointer} has been created(if it has been associated) - applicable
     *         for write operations only
     */
    public Object getObject() {
        return object;
    }

    /**
     * check whether entry associated with this pointer has been expired.
     * 
     * @param address
     *            off-heap memory address
     * @param offHeapMemoryManager
     *            off-heap memory manager
     * @return true if pointer's data is expired, otherwise false
     */
    public static boolean isExpired(final long address,
                                    final EffectiveMemoryManager offHeapMemoryManager) {
        boolean expired = false;

        long currentTimeMillis = System.currentTimeMillis();
        long timeToLive = getTimeToLive( address, offHeapMemoryManager );
        long creationTimestamp = getCreationTimestamp( address, offHeapMemoryManager );

        if ( currentTimeMillis >= timeToLive + creationTimestamp )
            expired = true;
        return expired;
    }

    /**
     * reallocate memory, dump content and associated meta information at the given address (this is for entry override
     * cases)
     * 
     * @param offHeapAddress
     *            off-heap memory address
     * @return new address after the memory reallocation
     */
    public long rellocateAndDump(final long offHeapAddress) {
        int bytesNeeded = getSerializedData().length + FormatFields.DATA.offset;
        this.address = memoryManager.reallocateMemory( offHeapAddress, bytesNeeded );
        flush2offheap();
        return this.address;
    }

    /**
     * allocate memory, dump(flush) content and associated technical information
     * 
     * @return new address after memory allocation
     */
    public long dumpAndGetAddress() {
        if ( this.address == 0 ) {
            int bytesNeeded = getSerializedData().length + FormatFields.DATA.offset;
            this.address = memoryManager.allocateMemory( bytesNeeded );
            flush2offheap();
        }
        return this.address;
    }

    private void flush2offheap() {
        long now = System.currentTimeMillis();
        memoryManager.putInt( address + FormatFields.LENGTH.offset, getSerializedData().length );
        memoryManager.putLong( address + FormatFields.CREATION_TIMESTAMP.offset, now );
        memoryManager.putInt( address + FormatFields.TIME_TO_LIVE.offset, ttl );
        memoryManager.putLong( address + FormatFields.LAST_ACCESS_DATE.offset, now );
        memoryManager.writeBytesArray( address + FormatFields.DATA.offset, getSerializedData() );
    }

    private long getAddress() {
        Preconditions.checkArgument( address > 0, "pointer is not initialized" );
        return address;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode( getAddress() );
    }

    @Override
    public boolean equals(final Object obj) {
        if ( obj instanceof ByteArrayPointer )
            return getAddress() == ( (ByteArrayPointer) obj ).getAddress();
        return super.equals( obj );
    }

    @Override
    public String toString() {
        return Objects.toStringHelper( this ).add( "address", getAddress() ).toString();
    }
}
