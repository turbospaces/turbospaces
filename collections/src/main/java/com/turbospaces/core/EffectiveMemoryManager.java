package com.turbospaces.core;

/**
 * effective memory manager interface is actually abstraction around allocating/destroying/reallocating off-heap memory.
 * 
 * @since 0.1
 */
public interface EffectiveMemoryManager {
    /**
     * allocate given amount of bytes in the off-heap memory store.
     * 
     * @param bytes - how many bytes to allocate
     * @return the unique pointer that later can be used for reading underlying data back
     */
    long allocateMemory(int bytes);

    /**
     * free(release) the underlying bytes occupied under given pointer address.
     * 
     * @param pointer
     *            off-heap memory pointer
     */
    void freeMemory(long pointer);

    /**
     * re-allocate memory at the given address and extends to newSize
     * 
     * @param address
     *            existing off-heap memory address
     * @param newSize
     *            how many bytes required now
     * @return address(pointer) itself
     */
    long reallocateMemory(long address,
                          int newSize);

    /**
     * write byte array into off-heap memory at the given address. This method is not optimal to fit best performance,
     * however is consistent and simple. Potentially bulk write will be used in future releases to get best performance.
     * 
     * @param address
     *            off-heap memory address
     * @param arr
     *            bytes to write
     */
    void writeBytesArray(long address,
                         byte[] arr);

    /**
     * write int value at the given off-heap memory address
     * 
     * @param address
     *            off-heap memory address
     * @param value
     *            int value
     */
    void putInt(long address,
                final int value);

    /**
     * write long value at the given off-heap memory address
     * 
     * @param address
     *            off-heap memory address
     * @param value
     *            long value
     */
    void putLong(long address,
                 long value);

    /**
     * read int value at the at given off-heap address
     * 
     * @param address
     *            off-heap memory address
     * @return int value
     */
    int getInt(final long address);

    /**
     * read long value at the given off-heap memory address
     * 
     * @param address
     *            off-heap memory address
     * @return long value
     */
    long getLong(long address);

    /**
     * read byte array at the given off-heap memory address.
     * 
     * @param address
     *            off-heap memory address
     * @param size
     *            Exact byte's array size
     * @return byte array
     */
    byte[] readBytesArray(long address,
                          int size);
}
