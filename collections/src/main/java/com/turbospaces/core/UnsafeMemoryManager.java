package com.turbospaces.core;

import com.lmax.disruptor.util.Util;

/**
 * Concrete implementation of memory manager over SUN's unsafe.
 * 
 * @since 0.1
 */
public final class UnsafeMemoryManager implements EffectiveMemoryManager {
    @Override
    public long allocateMemory(final int bytes) {
        return Util.getUnsafe().allocateMemory( bytes );
    }

    @Override
    public void freeMemory(final long pointer) {
        assert pointer > 0;
        Util.getUnsafe().freeMemory( pointer );
    }

    @Override
    public long reallocateMemory(final long address,
                                 final int newSize) {
        assert address > 0;
        return Util.getUnsafe().reallocateMemory( address, newSize );
    }

    @Override
    public void writeBytesArray(final long address,
                                final byte[] arr) {
        assert address > 0;
        long i = address;
        for ( final byte b : arr )
            Util.getUnsafe().putByte( i++, b );
    }

    @Override
    public void putInt(final long address,
                       final int value) {
        assert address > 0;
        Util.getUnsafe().putInt( address, value );
    }

    @Override
    public void putLong(final long address,
                        final long value) {
        assert address > 0;
        Util.getUnsafe().putLong( address, value );
    }

    @Override
    public int getInt(final long address) {
        assert address > 0;
        return Util.getUnsafe().getInt( address );
    }

    @Override
    public long getLong(final long address) {
        assert address > 0;
        return Util.getUnsafe().getLong( address );
    }

    @Override
    public byte[] readBytesArray(final long address,
                                 final int size) {
        assert address > 0;
        long j = address;
        byte[] arr = new byte[size];
        for ( int i = 0; i < size; i++ )
            arr[i] = Util.getUnsafe().getByte( j++ );
        return arr;
    }
}
