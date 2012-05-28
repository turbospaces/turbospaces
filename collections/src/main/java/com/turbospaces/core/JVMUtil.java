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

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import sun.misc.Unsafe;

import com.esotericsoftware.kryo.ObjectBuffer;
import com.esotericsoftware.minlog.Log;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import com.turbospaces.pool.ObjectFactory;
import com.turbospaces.pool.ObjectPool;
import com.turbospaces.pool.SimpleObjectPool;

/**
 * This is placeholder for all utility method of turbospaces-collections maven project. We prefer creation of one such
 * global utility class (something to jgroup's Util) rather than multiple small classes in-place. You can still
 * argue...
 * 
 * @since 0.1
 */
@SuppressWarnings("restriction")
public class JVMUtil {
    private static final Unsafe UNSAFE;

    static {
        UNSAFE = AccessController.doPrivileged( new PrivilegedAction<Unsafe>() {
            @Override
            public Unsafe run() {
                Unsafe unsafe = null;
                try {
                    Field theUnsafeInstance = Unsafe.class.getDeclaredField( "theUnsafe" );
                    theUnsafeInstance.setAccessible( true );
                    unsafe = (Unsafe) theUnsafeInstance.get( Unsafe.class );
                }
                catch ( Exception e ) {
                    Log.error( e.getMessage(), e );
                    Throwables.propagate( e );
                }
                return unsafe;
            }
        } );
    }

    /**
     * This method guarantees that garbage collection is done unlike <code>{@link System#gc()}</code>. Please be careful
     * with extra usage of this method, we created it for testing purposes mostly.
     */
    public static void gc() {
        Object obj = new Object();
        WeakReference<Object> ref = new WeakReference<Object>( obj );
        obj = null;
        while ( ref.get() != null )
            System.gc();
    }

    /**
     * This method guarantees that garbage collection is done after JVM shutdown triggering but before JVM
     * shutdown.Please be careful
     * with extra usage of this method, we created it for testing purposes mostly.
     */
    public static void gcOnExit() {
        Runtime.getRuntime().addShutdownHook( new Thread() {
            @Override
            public void run() {
                gc();
            }
        } );
    }

    /**
     * Allocate an instance but do not run any constructor.
     * Initializes the class if it has not yet been.
     * 
     * @param clazz
     *            type of new instance
     * @return new instance of class
     * @throws InstantiationException
     *             re-throw exceptions
     */
    @SuppressWarnings("unchecked")
    public static <T> T newInstance(final Class<T> clazz)
                                                         throws InstantiationException {
        return (T) UNSAFE.allocateInstance( clazz );
    }

    /**
     * allocate off-heap memory and returns the address of the memory.
     * 
     * @param size
     *            how many off-heap bytes required
     * @return address in OS memory
     */
    public static long allocateMemory(final int size) {
        return UNSAFE.allocateMemory( size );
    }

    /**
     * free(release) off-heap memory (like C destructor)
     * 
     * @param address
     *            off-heap address
     */
    public static void releaseMemory(final long address) {
        UNSAFE.freeMemory( address );
    }

    /**
     * re-allocate memory at the given address and extend to newSize
     * 
     * @param address
     *            existing off-heap memory address
     * @param newSize
     *            how many bytes required now
     * @return address itself
     */
    public static long reallocate(final long address,
                                  final int newSize) {
        return UNSAFE.reallocateMemory( address, newSize );
    }

    /**
     * write byte array into off-heap memory at the given address. This method is not optimal to fit best performance,
     * however is consistent and simple. Potentially bulk write will be used in future releases to get best performance.
     * 
     * @param address
     *            off-heap memory address
     * @param arr
     *            bytes to write
     */
    public static void writeBytesArray(final long address,
                                       final byte[] arr) {
        long i = address;
        for ( final byte b : arr )
            UNSAFE.putByte( i++, b );
    }

    /**
     * write int value at the given off-heap memory address
     * 
     * @param address
     *            off-heap memory address
     * @param value
     *            int value
     */
    public static void putInt(final long address,
                              final int value) {
        UNSAFE.putInt( address, value );
    }

    /**
     * write long value at the given off-heap memory
     * 
     * @param address
     *            off-heap memory address
     * @param value
     *            long value
     * 
     */
    public static void putLong(final long address,
                               final long value) {
        UNSAFE.putLong( address, value );
    }

    /**
     * read int value at the at given off-heap address
     * 
     * @param address
     *            off-heap memory address
     * @return int value
     */
    public static int getInt(final long address) {
        return UNSAFE.getInt( address );
    }

    /**
     * read long value at the given off-heap memory address
     * 
     * @param address
     *            off-heap memory address
     * @return long value
     */
    public static long getLong(final long address) {
        return UNSAFE.getLong( address );
    }

    /**
     * read byte array at the given off-heap memory address.
     * 
     * @param address
     *            off-heap memory address
     * @param size
     *            Exact byte's array size
     * @return byte array
     */
    public static byte[] readBytesArray(final long address,
                                        final int size) {
        long j = address;
        byte[] arr = new byte[size];
        for ( int i = 0; i < size; i++ )
            arr[i] = UNSAFE.getByte( j++ );
        return arr;
    }

    /**
     * start new thread and execute client's runnable task. Wait for thread completition. Catch execution exception and
     * return it. If there is not such execution exception, raise {@link AssertionError}.</p>
     * 
     * @param runnable
     *            client's task
     * @return execution exception if any
     */
    public static Exception runAndGetExecutionException(final Runnable runnable) {
        final MutableObject<Exception> ex = new MutableObject<Exception>();
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    runnable.run();
                }
                catch ( Exception e ) {
                    Log.error( e.getMessage(), e );
                    ex.set( e );
                }
            }
        };
        thread.start();
        Uninterruptibles.joinUninterruptibly( thread );
        if ( ex.get() == null )
            throw new AssertionError( "there is no exception!" );
        return ex.get();
    }

    /**
     * repeats the task action totalIterationsCount times concurrently(you provide how many threads and callback
     * function) - this is general purpose utility.</p>
     * 
     * <strong>NOTE :</strong> this method returns all caught exceptions and you should at least use
     * <code>JUnit.Asser.assertTrue(repeateConcurrenlty.size(), 0)</code> or something similar to check that there are
     * no execution errors.
     * 
     * @param threads
     *            number of concurrent threads
     * @param totalIterationsCount
     *            how many times to repeat task execution concurrently
     * @param task
     *            the action which needs to be performed
     * @return all errors from thread's execution
     */
    public static <T> List<Throwable> repeatConcurrently(final int threads,
                                                         final int totalIterationsCount,
                                                         final Function<Integer, Object> task) {
        final AtomicInteger atomicLong = new AtomicInteger( totalIterationsCount );
        final CountDownLatch countDownLatch = new CountDownLatch( threads );
        final LinkedList<Throwable> errors = Lists.newLinkedList();
        for ( int j = 0; j < threads; j++ ) {
            Thread thread = new Thread( new Runnable() {

                @Override
                public void run() {
                    try {
                        int l;
                        while ( ( l = atomicLong.decrementAndGet() ) >= 0 )
                            try {
                                task.apply( l );
                            }
                            catch ( Throwable e ) {
                                Log.error( e.getMessage(), e );
                                errors.add( e );
                                Throwables.propagate( e );
                            }
                    }
                    finally {
                        countDownLatch.countDown();
                    }
                }
            } );
            thread.setName( String.format( "RepeateConcurrentlyThread-%s:{%s}", j, task.toString() ) );
            thread.start();
        }
        Uninterruptibles.awaitUninterruptibly( countDownLatch );
        return errors;
    }

    /**
     * repeats the task action totalIterationsCount times concurrently - similar to
     * {@link #repeatConcurrently(int, int, Function)} but you can pass {@link Runnable} instead of {@link Function}.
     * 
     * @param threads
     *            number of concurrent threads
     * @param totalIterationsCount
     *            how many times to repeat task execution concurrently
     * @param task
     *            the action which needs to be performed (runnable task)
     * @return all errors from thread's execution
     * @see #repeatConcurrently(int, int, Function)
     */
    public static <T> List<Throwable> repeatConcurrently(final int threads,
                                                         final int totalIterationsCount,
                                                         final Runnable task) {
        return repeatConcurrently( threads, totalIterationsCount, new Function<Integer, Object>() {

            @Override
            public Object apply(final Integer iteration) {
                task.run();
                return this;
            }
        } );
    }

    /**
     * re-hashes a 4-byte sequence (Java int) using murmur3 hash algorithm.</p>
     * 
     * @param hash
     *            bad quality hash
     * @return good general purpose hash
     */
    public static int murmurRehash(final int hash) {
        int k = hash;

        k ^= k >>> 16;
        k *= 0x85ebca6b;
        k ^= k >>> 13;
        k *= 0xc2b2ae35;
        k ^= k >>> 16;

        return k;
    }

    /**
     * re-hashes a 4-byte sequence (Java int) using standard JDK re-hash algorithm.</p>
     * 
     * @param hash
     *            bad quality hash
     * @return good hash
     */
    public static int jdkRehash(final int hash) {
        int h = hash;

        h += h << 15 ^ 0xffffcd7d;
        h ^= h >>> 10;
        h += h << 3;
        h ^= h >>> 6;
        h += ( h << 2 ) + ( h << 14 );

        return h ^ h >>> 16;
    }

    /**
     * @return new {@link ObjectPool} pool for not-thread safe {@link ObjectBuffer} instances.
     */
    public static ObjectPool<ObjectBuffer> newObjectBufferPool() {
        SimpleObjectPool<ObjectBuffer> simpleObjectPool = new SimpleObjectPool<ObjectBuffer>( new ObjectFactory<ObjectBuffer>() {
            @Override
            public ObjectBuffer newInstance() {
                return new ObjectBuffer( null, 4 * 1024, 32 * 1024 );
            }

            @Override
            public void invalidate(final ObjectBuffer obj) {
                obj.setKryo( null );
            }
        } );
        simpleObjectPool.setMaxElements( 1 << 4 );
        return simpleObjectPool;
    }

    /**
     * Determine if the given objects are equal, returning <code>true</code> if both are <code>null</code> or
     * <code>false</code> if only one is <code>null</code>.
     * <p>
     * Compares arrays with <code>Arrays.equals</code>, performing an equality check based on the array elements rather
     * than the array reference.
     * 
     * @param o1
     *            first Object to compare
     * @param o2
     *            second Object to compare
     * @return whether the given objects are equal
     * @see java.util.Arrays#equals
     */
    public static boolean equals(final Object o1,
                                 final Object o2) {
        if ( o1 == o2 )
            return true;
        if ( o1 == null || o2 == null )
            return false;
        if ( o1.equals( o2 ) )
            return true;
        if ( o1.getClass().isArray() && o2.getClass().isArray() ) {
            if ( o1 instanceof Object[] && o2 instanceof Object[] )
                return Arrays.equals( (Object[]) o1, (Object[]) o2 );
            if ( o1 instanceof boolean[] && o2 instanceof boolean[] )
                return Arrays.equals( (boolean[]) o1, (boolean[]) o2 );
            if ( o1 instanceof byte[] && o2 instanceof byte[] )
                return Arrays.equals( (byte[]) o1, (byte[]) o2 );
            if ( o1 instanceof char[] && o2 instanceof char[] )
                return Arrays.equals( (char[]) o1, (char[]) o2 );
            if ( o1 instanceof double[] && o2 instanceof double[] )
                return Arrays.equals( (double[]) o1, (double[]) o2 );
            if ( o1 instanceof float[] && o2 instanceof float[] )
                return Arrays.equals( (float[]) o1, (float[]) o2 );
            if ( o1 instanceof int[] && o2 instanceof int[] )
                return Arrays.equals( (int[]) o1, (int[]) o2 );
            if ( o1 instanceof long[] && o2 instanceof long[] )
                return Arrays.equals( (long[]) o1, (long[]) o2 );
            if ( o1 instanceof short[] && o2 instanceof short[] )
                return Arrays.equals( (short[]) o1, (short[]) o2 );
        }
        return false;
    }

    private JVMUtil() {}
}
