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
package com.turbospaces.offmemory;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.Immutable;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.turbospaces.api.JSpace;
import com.turbospaces.core.SpaceUtility;

/**
 * off-heap memory pointer (basically OS memory address and bytes occupied).
 * 
 * @since 0.1
 */
@Immutable
public final class ByteArrayPointer {
    private static final int INT_SIZE = Integer.SIZE / Byte.SIZE;
    private static final int LONG_SIZE = Long.SIZE / Byte.SIZE;

    /**
     * Entities are stored in special internal format (it is quite trivial). There is nothing special in this format and
     * intention is to store the maximum meta-information co-located with actual entitie's data array rather than
     * storing in
     * memory.
     * </p>
     * 
     * <h3>Format is:</h3>
     * <ul>
     * <li>length - int</li>
     * <li>creationTimestamp - long</li>
     * <li>timeToLive - long</li>
     * <li>hitsCount - long</li>
     * <li>actual entitie's state - byte array of serializable entity</li>
     * </ul>
     * 
     * @since 0.1
     */
    private static enum FormatFields {
        LENGTH(INT_SIZE),
        CREATION_TIMESTAMP(LONG_SIZE),
        TIME_TO_LIVE(LONG_SIZE),
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

    private byte[] serializedBytes;
    private ByteBuffer serializedData;
    private long address;
    private Object object;
    private long ttl;

    /**
     * create new byte array pointer from given address and byte array(buffer).
     * 
     * @param address
     * @param serializedData
     */
    public ByteArrayPointer(final long address, final ByteBuffer serializedData) {
        assert serializedData != null;

        this.address = address;
        this.serializedData = serializedData;
    }

    /**
     * create new Byte Array pointer for the given array and write byte array to the off-heap memory.
     * 
     * @param serializedData
     *            serialized entiti'es state
     * @param object
     *            target object (this {@link ByteArrayPointer} created over object)
     * @param ttl
     *            time-to-live
     */
    public ByteArrayPointer(final byte[] serializedData, final Object object, final long ttl) {
        assert serializedData != null;

        this.object = object;
        this.serializedBytes = serializedData;
        this.ttl = ttl;
    }

    /**
     * extract actual state of entity (without meta information)
     * 
     * @param address
     * 
     * @return buffer over internal state
     */
    public static byte[] getEntityState(final long address) {
        return SpaceUtility.readBytesArray( address + FormatFields.DATA.offset, getBytesOccupied( address ) );
    }

    /**
     * extract how many bytes are being occupied by underlying de-serialized entities data.
     * 
     * @param address
     * @return how many bytes occupied by pointer
     */
    public static int getBytesOccupied(final long address) {
        return SpaceUtility.getInt( address + FormatFields.LENGTH.offset );
    }

    /**
     * get the creation timestamp for associated space store entity
     * 
     * @param address
     * @return creation timestamp
     */
    public static long getCreationTimestamp(final long address) {
        return SpaceUtility.getLong( address + FormatFields.CREATION_TIMESTAMP.offset );
    }

    /**
     * get the time-to-live for associated space store entity
     * 
     * @param address
     * @return ttl
     */
    public static long getTimeToLive(final long address) {
        return SpaceUtility.getLong( address + FormatFields.TIME_TO_LIVE.offset );
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
     * utilized used resources
     */
    public void utilize() {
        if ( address != 0 )
            SpaceUtility.releaseMemory( address );
    }

    /**
     * @return how many off-heap bytes are being occupied by this pointer
     */
    public int bytesOccupied() {
        return getSerializedData().length;
    }

    /**
     * @return true if pointer's data is expired
     */
    public boolean isExpired() {
        return isExpired( getAddress() );
    }

    /**
     * @return object for which this {@link ByteArrayPointer} has been created
     */
    public Object getObject() {
        return object;
    }

    /**
     * test whether entity associated with this pointer has been expired.
     * 
     * @param address
     * @return true if pointer's data is expired
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
     * reallocate memory, dump content and associated technical information at given address
     * 
     * @param offHeapAddress
     * @return new address after the memory reallocation
     */
    public long rellocateAndDump(final long offHeapAddress) {
        int bytesNeeded = getSerializedData().length + FormatFields.DATA.offset;
        this.address = SpaceUtility.reallocate( offHeapAddress, bytesNeeded );
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
            this.address = SpaceUtility.allocateMemory( bytesNeeded );
            flush2offheap();
        }
        return this.address;
    }

    private void flush2offheap() {
        SpaceUtility.putInt( address + FormatFields.LENGTH.offset, getSerializedData().length );
        SpaceUtility.putLong( address + FormatFields.CREATION_TIMESTAMP.offset, System.currentTimeMillis() );
        SpaceUtility.putLong( address + FormatFields.TIME_TO_LIVE.offset, ttl >= JSpace.LEASE_FOREVER ? JSpace.LEASE_FOREVER : ttl );
        SpaceUtility.writeBytesArray( address + FormatFields.DATA.offset, getSerializedData() );
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
