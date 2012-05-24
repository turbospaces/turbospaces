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
import com.turbospaces.core.JVMUtil;

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
        DATA(Integer.MAX_VALUE);

        private FormatFields(final int lenght) {
            this.lenght = lenght;
        }

        private final int lenght;
        private int offset;
    }

    static {
        int offset = 0;
        for ( FormatFields f : FormatFields.values() ) {
            f.offset = offset;
            offset += f.lenght;
        }
    }
    private static int INTERNAL_BYTES_OCCUPATION = FormatFields.LENGTH.lenght + FormatFields.CREATION_TIMESTAMP.lenght
            + FormatFields.TIME_TO_LIVE.lenght;

    private byte[] serializedBytes;
    private ByteBuffer serializedData;
    private long address;
    private Object object;
    private int ttl;

    /**
     * create new byte array pointer at given address and byte array(buffer) - this is constructor is used for reading
     * from off-heap memory.
     * 
     * @param address
     *            off-heap memory address
     * @param serializedData
     *            actual de-serialized state of the entity(entry)
     */
    public ByteArrayPointer(final long address, final ByteBuffer serializedData) {
        assert serializedData != null;

        this.address = address;
        this.serializedData = serializedData;
    }

    /**
     * create new byte array pointer for the given de-serialized entry and associate original entry(as reference) with
     * this pointer. Also time-to-live must be provided.
     * 
     * @param serializedData
     *            serialized entiti'es state
     * @param object
     *            target object (this {@link ByteArrayPointer} created over object)
     * @param ttl
     *            time-to-live
     */
    public ByteArrayPointer(final byte[] serializedData, final Object object, final int ttl) {
        assert serializedData != null;

        this.object = object;
        this.serializedBytes = serializedData;
        this.ttl = ttl;
    }

    /**
     * read actual state of entity (without meta information) at the given address
     * 
     * @param address
     *            off-heap memory address
     * @return buffer over internal state
     */
    public static byte[] getEntityState(final long address) {
        return JVMUtil.readBytesArray( address + FormatFields.DATA.offset, JVMUtil.getInt( address + FormatFields.LENGTH.offset ) );
    }

    /**
     * read how many bytes are being occupied by underlying de-serialized entry's data.
     * 
     * @param address
     *            off-heap memory address
     * @return how many bytes occupied by pointer
     */
    public static int getBytesOccupied(final long address) {
        return JVMUtil.getInt( address + FormatFields.LENGTH.offset ) + INTERNAL_BYTES_OCCUPATION;
    }

    /**
     * read the creation timestamp of the associated space store entry(when the entry has been added).
     * 
     * @param address
     *            off-heap memory address
     * @return creation timestamp
     */
    public static long getCreationTimestamp(final long address) {
        return JVMUtil.getLong( address + FormatFields.CREATION_TIMESTAMP.offset );
    }

    /**
     * get the time-to-live for associated space store entity(how long to keep entry living)
     * 
     * @param address
     *            off-heap memory address
     * @return ttl time-to-live associate with entry
     */
    public static int getTimeToLive(final long address) {
        return JVMUtil.getInt( address + FormatFields.TIME_TO_LIVE.offset );
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
            JVMUtil.releaseMemory( address );
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
        return isExpired( getAddress() );
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
     * @return true if pointer's data is expired, otherwise false
     */
    public static boolean isExpired(final long address) {
        boolean expired = false;

        long currentTimeMillis = System.currentTimeMillis();
        long timeToLive = getTimeToLive( address );
        long creationTimestamp = getCreationTimestamp( address );

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
        this.address = JVMUtil.reallocate( offHeapAddress, bytesNeeded );
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
            this.address = JVMUtil.allocateMemory( bytesNeeded );
            flush2offheap();
        }
        return this.address;
    }

    private void flush2offheap() {
        JVMUtil.putInt( address + FormatFields.LENGTH.offset, getSerializedData().length );
        JVMUtil.putLong( address + FormatFields.CREATION_TIMESTAMP.offset, System.currentTimeMillis() );
        JVMUtil.putInt( address + FormatFields.TIME_TO_LIVE.offset, ttl );
        JVMUtil.writeBytesArray( address + FormatFields.DATA.offset, getSerializedData() );
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
