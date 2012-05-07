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
package com.turbospaces.core;

import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.data.mapping.model.MutablePersistentEntity;
import org.springframework.util.ObjectUtils;

import sun.misc.Unsafe;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serialize.BigDecimalSerializer;
import com.esotericsoftware.kryo.serialize.BigIntegerSerializer;
import com.esotericsoftware.kryo.serialize.CollectionSerializer;
import com.esotericsoftware.kryo.serialize.DateSerializer;
import com.esotericsoftware.kryo.serialize.EnumSerializer;
import com.esotericsoftware.kryo.serialize.FieldSerializer;
import com.esotericsoftware.kryo.serialize.MapSerializer;
import com.esotericsoftware.kryo.serialize.SimpleSerializer;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.turbospaces.api.AbstractSpaceConfiguration;
import com.turbospaces.api.CapacityRestriction;
import com.turbospaces.api.JSpace;
import com.turbospaces.api.SpaceCapacityOverflowException;
import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.api.SpaceErrors;
import com.turbospaces.api.SpaceException;
import com.turbospaces.api.SpaceMemoryOverflowException;
import com.turbospaces.api.SpaceOperation;
import com.turbospaces.api.SpaceTopology;
import com.turbospaces.collections.OffHeapHashSet;
import com.turbospaces.collections.OffHeapLinearProbingSet;
import com.turbospaces.model.BO;
import com.turbospaces.network.MethodCall.BeginTransactionMethodCall;
import com.turbospaces.network.MethodCall.CommitRollbackMethodCall;
import com.turbospaces.network.MethodCall.FetchMethodCall;
import com.turbospaces.network.MethodCall.GetMbUsedMethodCall;
import com.turbospaces.network.MethodCall.GetSizeMethodCall;
import com.turbospaces.network.MethodCall.GetSpaceTopologyMethodCall;
import com.turbospaces.network.MethodCall.NotifyListenerMethodCall;
import com.turbospaces.network.MethodCall.WriteMethodCall;
import com.turbospaces.offmemory.ByteArrayPointer;
import com.turbospaces.pool.ObjectFactory;
import com.turbospaces.pool.ObjectPool;
import com.turbospaces.pool.SimpleObjectPool;
import com.turbospaces.serialization.NetworkCommunicationSerializer;
import com.turbospaces.serialization.PropertiesSerializer;
import com.turbospaces.serialization.SingleDimensionArraySerializer;
import com.turbospaces.spaces.CacheStoreEntryWrapper;
import com.turbospaces.spaces.EntryKeyLockQuard;
import com.turbospaces.spaces.KeyLocker;
import com.turbospaces.spaces.tx.TransactionScopeKeyLocker;

/**
 * Space utils placeholder.
 * 
 * @since 0.1
 */
@ThreadSafe
@SuppressWarnings({ "unchecked", "restriction" })
public abstract class SpaceUtility {
    private static final Logger LOGGER = LoggerFactory.getLogger( SpaceUtility.class );

    private static final Properties cloudProperties;
    private static final Unsafe unsafe;

    static {
        LOGGER.trace( "initializing {}", ByteArrayPointer.class.toString() );

        unsafe = AccessController.doPrivileged( new PrivilegedAction<Unsafe>() {
            @Override
            public Unsafe run() {
                return SpaceUtility.exceptionShouldNotHappen( new Callable<Unsafe>() {
                    @Override
                    public Unsafe call()
                                        throws Exception {
                        Field theUnsafeInstance;

                        theUnsafeInstance = Unsafe.class.getDeclaredField( "theUnsafe" );
                        theUnsafeInstance.setAccessible( true );
                        Unsafe u = (Unsafe) theUnsafeInstance.get( Unsafe.class );

                        return u;
                    }
                } );
            }
        } );
        cloudProperties = SpaceUtility.exceptionShouldNotHappen( new Callable<Properties>() {
            @Override
            public Properties call()
                                    throws Exception {
                ClassPathResource resource = new ClassPathResource( "turbospaces.properties" );
                Properties properties = new Properties();
                InputStream inputStream = null;
                try {
                    inputStream = resource.getInputStream();
                    properties.load( inputStream );
                }
                finally {
                    if ( inputStream != null )
                        inputStream.close();
                }
                return properties;
            }
        } );
    }

    /**
     * This method guarantees that garbage collection is done unlike <code>{@link System#gc()}</code>.
     * You should not worry that this method can cause some kind of infinite loop because there is pre-defined max
     * iteration number.
     */
    public static void gc() {
        Object obj = new Object();
        WeakReference<Object> ref = new WeakReference<Object>( obj );
        obj = null;
        while ( ref.get() != null )
            System.gc();
    }

    /**
     * This method guarantees that garbage collection is done after JVM shutdown is initialized
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
     * check whether template object(in form of property values) matches with actual bean's property value. This method
     * is efficiently used for {@link JSpace#notify(Object, com.turbospaces.api.SpaceNotificationListener, int)}
     * operation
     * implementation.
     * 
     * @param templatePropertyValues
     * @param entryPropertyValues
     * 
     * @return template matches result
     */
    public static boolean macthesByPropertyValues(final Object[] templatePropertyValues,
                                                  final Object[] entryPropertyValues) {
        assert templatePropertyValues.length == entryPropertyValues.length;

        for ( int i = 0; i < templatePropertyValues.length; i++ ) {
            final Object templatePropertyValue = templatePropertyValues[i];
            final Object entryPropertyValue = entryPropertyValues[i];

            if ( templatePropertyValue != null && !ObjectUtils.nullSafeEquals( templatePropertyValue, entryPropertyValue ) )
                return false;
        }
        return true;
    }

    /**
     * @param configuration
     * @param givenKryo
     *            user custom kryo serializer
     * @return kryo with pre-registered application types and some defaults.
     * @throws ClassNotFoundException
     */
    @SuppressWarnings("rawtypes")
    public static Kryo spaceKryo(final AbstractSpaceConfiguration configuration,
                                 final Kryo givenKryo)
                                                      throws ClassNotFoundException {
        final Map<Class<?>, Serializer> serializers = new LinkedHashMap<Class<?>, Serializer>();
        final Kryo kryo = givenKryo == null ? new Kryo() {
            @Override
            public String toString() {
                StringBuilder builder = new StringBuilder();
                builder.append( "serializers) = " ).append( "\n" );
                for ( Entry<Class<?>, Serializer> entry : serializers.entrySet() ) {
                    Class<?> key = entry.getKey();
                    builder.append( "\t" );
                    builder.append( key.getName() + " -> " + entry.getValue() );
                    builder.append( "\n" );
                }
                return "Kryo(" + builder.toString();
            }
        } : givenKryo;

        serializers.put( SpaceOperation.class, new EnumSerializer( SpaceOperation.class ) );
        serializers.put( SpaceTopology.class, new EnumSerializer( SpaceTopology.class ) );
        serializers.put( Date.class, new DateSerializer() );

        serializers.put( BigDecimal.class, new BigDecimalSerializer() );
        serializers.put( BigInteger.class, new BigIntegerSerializer() );

        serializers.put( Map.class, new MapSerializer( kryo ) );
        serializers.put( List.class, new CollectionSerializer( kryo ) );
        serializers.put( Set.class, new CollectionSerializer( kryo ) );
        serializers.put( HashMap.class, new MapSerializer( kryo ) );

        serializers.put( TreeMap.class, new MapSerializer( kryo ) );
        serializers.put( LinkedHashMap.class, new MapSerializer( kryo ) );
        serializers.put( Hashtable.class, new MapSerializer( kryo ) );
        serializers.put( Properties.class, new MapSerializer( kryo ) );
        serializers.put( ArrayList.class, new CollectionSerializer( kryo ) );
        serializers.put( LinkedList.class, new CollectionSerializer( kryo ) );
        serializers.put( HashSet.class, new CollectionSerializer( kryo ) );
        serializers.put( TreeSet.class, new CollectionSerializer( kryo ) );
        serializers.put( LinkedHashSet.class, new CollectionSerializer( kryo ) );

        serializers.put( boolean[].class, new SingleDimensionArraySerializer( boolean[].class, kryo ) );
        serializers.put( byte[].class, new SingleDimensionArraySerializer( byte[].class, kryo ) );
        serializers.put( short[].class, new SingleDimensionArraySerializer( short[].class, kryo ) );
        serializers.put( char[].class, new SingleDimensionArraySerializer( char[].class, kryo ) );
        serializers.put( int[].class, new SingleDimensionArraySerializer( int[].class, kryo ) );
        serializers.put( float[].class, new SingleDimensionArraySerializer( float[].class, kryo ) );
        serializers.put( double[].class, new SingleDimensionArraySerializer( double[].class, kryo ) );
        serializers.put( long[].class, new SingleDimensionArraySerializer( long[].class, kryo ) );

        serializers.put( byte[][].class, new NetworkCommunicationSerializer() );

        serializers.put( Boolean[].class, new SingleDimensionArraySerializer( Boolean[].class, kryo ) );
        serializers.put( Byte[].class, new SingleDimensionArraySerializer( Byte[].class, kryo ) );
        serializers.put( Short[].class, new SingleDimensionArraySerializer( Short[].class, kryo ) );
        serializers.put( Character[].class, new SingleDimensionArraySerializer( Character[].class, kryo ) );
        serializers.put( Integer[].class, new SingleDimensionArraySerializer( Integer[].class, kryo ) );
        serializers.put( Float[].class, new SingleDimensionArraySerializer( Float[].class, kryo ) );
        serializers.put( Double[].class, new SingleDimensionArraySerializer( Double[].class, kryo ) );
        serializers.put( Long[].class, new SingleDimensionArraySerializer( Long[].class, kryo ) );
        serializers.put( String[].class, new SingleDimensionArraySerializer( String[].class, kryo ) );

        serializers.put( ByteArrayPointer.class, new FieldSerializer( kryo, ByteArrayPointer.class ) );
        serializers.put( Throwable.class, new FieldSerializer( kryo, Throwable.class ) );

        serializers.put( com.turbospaces.network.MethodCall.class, new FieldSerializer( kryo, com.turbospaces.network.MethodCall.class ) );
        serializers.put( WriteMethodCall.class, new FieldSerializer( kryo, WriteMethodCall.class ) );
        serializers.put( FetchMethodCall.class, new FieldSerializer( kryo, FetchMethodCall.class ) );
        serializers.put( BeginTransactionMethodCall.class, new FieldSerializer( kryo, BeginTransactionMethodCall.class ) );
        serializers.put( CommitRollbackMethodCall.class, new FieldSerializer( kryo, CommitRollbackMethodCall.class ) );
        serializers.put( GetSpaceTopologyMethodCall.class, new FieldSerializer( kryo, GetSpaceTopologyMethodCall.class ) );
        serializers.put( GetMbUsedMethodCall.class, new FieldSerializer( kryo, GetMbUsedMethodCall.class ) );
        serializers.put( GetSizeMethodCall.class, new FieldSerializer( kryo, GetSizeMethodCall.class ) );
        serializers.put( NotifyListenerMethodCall.class, new FieldSerializer( kryo, NotifyListenerMethodCall.class ) );

        Collection<BasicPersistentEntity> persistentEntities = configuration.getMappingContext().getPersistentEntities();
        for ( BasicPersistentEntity e : persistentEntities ) {
            BO bo = configuration.boFor( e.getType() );
            bo.doWithProperties( new PropertyHandler() {

                @Override
                public void doWithPersistentProperty(final PersistentProperty p) {
                    Class type = p.getType();
                    if ( type.isArray() && !serializers.containsKey( type ) ) {
                        SingleDimensionArraySerializer serializer = new SingleDimensionArraySerializer( type, kryo );
                        kryo.register( type, serializer );
                        serializers.put( type, serializer );
                    }
                    else if ( type.isEnum() && !serializers.containsKey( type ) ) {
                        EnumSerializer enumSerializer = new EnumSerializer( type );
                        kryo.register( type, enumSerializer );
                        serializers.put( type, enumSerializer );
                    }
                }
            } );
            Class<?> arrayWrapperType = Class.forName( "[L" + e.getType().getName() + ";" );
            PropertiesSerializer serializer = new PropertiesSerializer( configuration, bo );
            SingleDimensionArraySerializer arraysSerializer = new SingleDimensionArraySerializer( arrayWrapperType, kryo );
            kryo.register( e.getType(), serializer );
            kryo.register( arrayWrapperType, arraysSerializer );
            serializers.put( e.getType(), serializer );
            serializers.put( arrayWrapperType, arraysSerializer );
        }

        serializers.put( CacheStoreEntryWrapper.class, new SimpleSerializer<CacheStoreEntryWrapper>() {

            @Override
            public CacheStoreEntryWrapper read(final ByteBuffer buffer) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void write(final ByteBuffer buffer,
                              final CacheStoreEntryWrapper entry) {
                Serializer serializer = kryo.getRegisteredClass( entry.getPersistentEntity().getType() ).getSerializer();
                ( (PropertiesSerializer) serializer ).write( buffer, entry );
            }
        } );

        for ( Entry<Class<?>, Serializer> entry : serializers.entrySet() )
            kryo.register( entry.getKey(), entry.getValue() );
        return kryo;
    }

    /**
     * allocate off-heap memory and returns the address of the memory.
     * 
     * @param size
     *            how many off-heap bytes required
     * @return address in VM memory
     */
    public static long allocateMemory(final int size) {
        return unsafe.allocateMemory( size );
    }

    /**
     * free off-heap memory (like C destructor)
     * 
     * @param address
     *            off-heap address
     */
    public static void releaseMemory(final long address) {
        unsafe.freeMemory( address );
    }

    /**
     * re-allocate memory at the given address and extend to newSize
     * 
     * @param address
     * @param newSize
     * @return address itself
     */
    public static long reallocate(final long address,
                                  final int newSize) {
        return unsafe.reallocateMemory( address, newSize );
    }

    /**
     * write byte array into off-heap memory using address pointer. This method is not optimized by performance, however
     * is consistent and simple.
     * 
     * @param address
     *            off-heap address
     * @param arr
     *            bytes to write
     */
    public static void writeBytesArray(final long address,
                                       final byte[] arr) {
        long i = address;
        for ( byte b : arr )
            unsafe.putByte( i++, b );
    }

    /**
     * write int value at the given off-heap memory
     * 
     * @param address
     * @param value
     */
    public static void putInt(final long address,
                              final int value) {
        unsafe.putInt( address, value );
    }

    /**
     * write long value at the given off-heap memory
     * 
     * @param address
     * @param value
     */
    public static void putLong(final long address,
                               final long value) {
        unsafe.putLong( address, value );
    }

    /**
     * read int value from the off-heap memory at given address
     * 
     * @param address
     * @return int value
     */
    public static int getInt(final long address) {
        return unsafe.getInt( address );
    }

    /**
     * read long value from the off-heap memory at given address
     * 
     * @param address
     * @return long value
     */
    public static long getLong(final long address) {
        return unsafe.getLong( address );
    }

    /**
     * read byte array from the off-heap memory using address pointer. you must pass expected size of the byte's array.
     * 
     * @param address
     *            off-heap memory address
     * @param size
     *            expected byte's array size
     * @return byte array
     */
    public static byte[] readBytesArray(final long address,
                                        final int size) {
        long j = address;
        byte[] arr = new byte[size];
        for ( int i = 0; i < size; i++ )
            arr[i] = unsafe.getByte( j++ );
        return arr;
    }

    /**
     * raise new {@link SpaceCapacityOverflowException} exception with given maxCapacity restriction and for object
     * which caused space overflow.
     * 
     * @param size
     * @param obj
     */
    public static void raiseSpaceCapacityOverflowException(final long size,
                                                           final Object obj) {
        throw new SpaceCapacityOverflowException( size, obj );
    }

    /**
     * raise new {@link DuplicateKeyException} exception for given uniqueIdentifier and persistent class.
     * 
     * @param uniqueIdentifier
     * @param persistentClass
     * @see SpaceErrors#DUPLICATE_KEY_VIOLATION
     */
    public static void raiseDuplicateException(final Object uniqueIdentifier,
                                               final Class<?> persistentClass) {
        throw new DuplicateKeyException( String.format(
                SpaceErrors.DUPLICATE_KEY_VIOLATION,
                persistentClass.getSimpleName(),
                uniqueIdentifier.toString() ) );
    }

    /**
     * raise new {@link DataRetrievalFailureException} exception for given uniqueIdentifier and persistent class.
     * 
     * @param uniqueIdentifier
     * @param persistentClass
     * @see SpaceErrors#ENTITY_IS_MISSING_FOR_UPDATE
     */
    public static void raiseObjectRetrieveFailureException(final Object uniqueIdentifier,
                                                           final Class<?> persistentClass) {
        throw new DataRetrievalFailureException( String.format( SpaceErrors.ENTITY_IS_MISSING_FOR_UPDATE, uniqueIdentifier ) );
    }

    /**
     * raise new {@link CannotAcquireLockException} exception for given identifier and timeout. ALso you need to pass
     * write or exclusive read type as last method parameter in order to format proper error message.
     * 
     * @param uniqueIdentifier
     * @param timeout
     * @param write
     * @see SpaceErrors#UNABLE_TO_ACQUIRE_LOCK
     */
    public static void raiseCannotAcquireLockException(final Object uniqueIdentifier,
                                                       final long timeout,
                                                       final boolean write) {
        String type = write ? "write" : "exclusive read";
        throw new CannotAcquireLockException( String.format( SpaceErrors.UNABLE_TO_ACQUIRE_LOCK, type, uniqueIdentifier, timeout ) );
    }

    /**
     * ensure that the space buffer does not violate space capacity and can add new byte array pointer.
     * 
     * @param pointer
     * @param capacityRestriction
     * @param memoryUsed
     */
    public static void ensureEnoughCapacity(final ByteArrayPointer pointer,
                                            final CapacityRestriction capacityRestriction,
                                            final AtomicLong memoryUsed) {
        if ( memoryUsed.get() + pointer.bytesOccupied() > Memory.mb( capacityRestriction.getMaxMemorySizeInMb() ) )
            throw new SpaceMemoryOverflowException( capacityRestriction.getMaxMemorySizeInMb(), pointer.getSerializedData() );
    }

    /**
     * ensure that the space buffer does not violate space capacity and can add new byte array pointer.
     * 
     * @param obj
     * @param capacityRestriction
     * @param itemsCount
     */
    public static void ensureEnoughCapacity(final Object obj,
                                            final CapacityRestriction capacityRestriction,
                                            final AtomicLong itemsCount) {
        if ( itemsCount.get() >= capacityRestriction.getMaxElements() )
            throw new SpaceCapacityOverflowException( capacityRestriction.getMaxElements(), obj );
    }

    /**
     * start new thread and execute client's runnable task. Wait for thread work completition. Catch execution exception
     * and return if any.
     * 
     * @param runnable
     *            client's task
     * @return execution exception if any
     * @throws InterruptedException
     */
    public static Exception runAndGetExecutionException(final Runnable runnable)
                                                                                throws InterruptedException {
        final AtomicReference<Exception> ex = new AtomicReference<Exception>();
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    runnable.run();
                }
                catch ( Exception e ) {
                    LOGGER.error( e.getMessage(), e );
                    ex.set( e );
                }
            }
        };
        thread.start();
        thread.join();
        if ( ex.get() == null )
            throw new AssertionError( "there is no exception!" );
        return ex.get();
    }

    /**
     * repeats the task action totalIterationsCount times concurrently.
     * 
     * @param threads
     *            number of concurrent threads
     * @param totalIterationsCount
     *            how many times to repeat task execution concurrently
     * @param task
     *            the action which needs to be performed
     * @return all errors from thread's execution
     * @throws InterruptedException
     *             if thread(threads) was/were interrupted
     */
    public static <T> LinkedList<Throwable> repeatConcurrently(final int threads,
                                                               final int totalIterationsCount,
                                                               final Function<Integer, Object> task)
                                                                                                    throws InterruptedException {
        final AtomicInteger atomicLong = new AtomicInteger( totalIterationsCount );
        final CountDownLatch countDownLatch = new CountDownLatch( threads );
        final LinkedList<Throwable> errors = new LinkedList<Throwable>();
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
                            catch ( Exception e ) {
                                LOGGER.error( e.getMessage(), e );
                                errors.add( e );
                                throw new SpaceException( e.getMessage(), e );
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
        countDownLatch.await();
        return errors;
    }

    @SuppressWarnings("javadoc")
    public static <T> LinkedList<Throwable> repeatConcurrently(final int threads,
                                                               final int totalIterationsCount,
                                                               final Runnable task)
                                                                                   throws InterruptedException {
        return repeatConcurrently( threads, totalIterationsCount, new Function<Integer, Object>() {

            @Override
            public Object apply(final Integer iteration) {
                task.run();
                return this;
            }
        } );
    }

    /**
     * execute callback action assuming that exception should not occur and transform exceptions into
     * {@link SpaceException}
     * 
     * @param callback
     * @return result of callback execution
     * @throws SpaceException
     *             if something went wrong
     */
    public static <R> R exceptionShouldNotHappen(final Callable<R> callback)
                                                                            throws SpaceException {
        Exception e = null;
        R result = null;
        try {
            result = callback.call();
        }
        catch ( Exception e1 ) {
            e = e1;
        }
        if ( e != null )
            throw new SpaceException( "unexpected exception", e );
        return result;
    }

    /**
     * execute callback action assuming that exception should not occur and transform exceptions into
     * {@link SpaceException}
     * 
     * @param callback
     * @param input
     * @return result of callback execution
     * @throws SpaceException
     *             if something went wrong
     */
    public static <I, R> R exceptionShouldNotHappen(final Function<I, R> callback,
                                                    final I input) {
        Exception e = null;
        R result = null;
        try {
            result = callback.apply( input );
        }
        catch ( Exception e1 ) {
            e = e1;
        }
        if ( e != null )
            throw new SpaceException( "unexpected exception", e );
        return result;
    }

    /**
     * wrap original off-heap linear set by applying load distribution hash function and make it more
     * concurrent(parallel)
     * 
     * @param configuration
     * @param mutablePersistentEntity
     * @param initialCapacity
     * @param concurrencyLevel
     * @return more concurrent set
     */
    public static OffHeapHashSet parallelizedOffLinearHashSet(final SpaceConfiguration configuration,
                                                              final MutablePersistentEntity<?, ?> mutablePersistentEntity,
                                                              final int initialCapacity,
                                                              final int concurrencyLevel) {
        OffHeapHashSet parallelSet = new OffHeapHashSet() {
            private final OffHeapLinearProbingSet[] segments;

            private final int concurrency;
            private final int segmentShift;
            private final int segmentMask;

            {
                int sshift = 0;
                int ssize = 1;
                while ( ssize < concurrencyLevel ) {
                    ++sshift;
                    ssize <<= 1;
                }
                concurrency = ssize;
                segmentShift = 32 - sshift;
                segmentMask = ssize - 1;
                segments = new OffHeapLinearProbingSet[concurrency];

                int c = initialCapacity / ssize;
                if ( c * ssize < initialCapacity )
                    ++c;
                int cap = 1;
                while ( cap < c )
                    cap <<= 1;

                for ( int i = 0; i < concurrency; i++ )
                    segments[i] = new OffHeapLinearProbingSet( Math.min( 1 << 4, cap ), configuration, mutablePersistentEntity );
            }

            @Override
            public int remove(final Object key) {
                return segmentFor( key ).remove( key );
            }

            @Override
            public int put(final Object key,
                           final ByteArrayPointer value) {
                return segmentFor( key ).put( key, value );
            }

            @Override
            public boolean contains(final Object key) {
                return segmentFor( key ).contains( key );
            }

            @Override
            public ByteArrayPointer getAsPointer(final Object key) {
                return segmentFor( key ).getAsPointer( key );
            }

            @Override
            public ByteBuffer getAsSerializedData(final Object key) {
                return segmentFor( key ).getAsSerializedData( key );
            }

            @Override
            public void afterPropertiesSet() {
                for ( int i = 0; i < concurrency; i++ )
                    segments[i].afterPropertiesSet();
            }

            @Override
            public void destroy() {
                for ( int i = 0; i < concurrency; i++ )
                    segments[i].destroy();
            }

            @Override
            public String toString() {
                StringBuilder builder = new StringBuilder();
                builder.append( "OffHeap LinerMap: segments" ).append( "\n" );
                for ( int i = 0; i < concurrency; i++ ) {
                    builder.append( "\t" );
                    builder.append( i ).append( "->" ).append( segments[i] );
                    builder.append( "\n" );
                }

                return builder.toString();
            }

            private OffHeapLinearProbingSet segmentFor(final Object key) {
                final int hash = jdkHash( key.hashCode() );
                return segments[hash >>> segmentShift & segmentMask];
            }
        };
        return parallelSet;
    }

    /**
     * wrap original key locker by applying load distribution hash function and make it more concurrent(parallel)
     * 
     * @param concurrencyLevel
     * @return parallel key locker
     */
    public static KeyLocker parallelizedKeyLocker(final int concurrencyLevel) {
        KeyLocker locker = new KeyLocker() {
            private final TransactionScopeKeyLocker[] segments;

            private final int concurrency;
            private final int segmentShift;
            private final int segmentMask;

            {
                int sshift = 0;
                int ssize = 1;
                while ( ssize < concurrencyLevel ) {
                    ++sshift;
                    ssize <<= 1;
                }
                concurrency = ssize;
                segmentShift = 32 - sshift;
                segmentMask = ssize - 1;
                segments = new TransactionScopeKeyLocker[concurrency];

                for ( int i = 0; i < concurrency; i++ )
                    segments[i] = new TransactionScopeKeyLocker();
            }

            @Override
            public void writeUnlock(final EntryKeyLockQuard guard,
                                    final long transactionID) {
                segmentFor( guard.getKey() ).writeUnlock( guard, transactionID );
            }

            @Override
            public EntryKeyLockQuard writeLock(final Object key,
                                               final long transactionID,
                                               final long timeout,
                                               final boolean strict) {
                return segmentFor( key ).writeLock( key, transactionID, timeout, strict );
            }

            private TransactionScopeKeyLocker segmentFor(final Object key) {
                final int hash = jdkHash( key.hashCode() );
                return segments[hash >>> segmentShift & segmentMask];
            }
        };

        return locker;
    }

    /**
     * @return new {@link ObjectPool} pool
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
     * Hashes a 4-byte sequence (Java int).
     * 
     * @param hash
     *            key hash
     * @return hash
     */
    public static int hash(final int hash) {
        int k = hash;
        k ^= k >>> 16;
        k *= 0x85ebca6b;
        k ^= k >>> 13;
        k *= 0xc2b2ae35;
        k ^= k >>> 16;
        return k;
    }

    private static int jdkHash(final int hash) {
        int h = hash;
        h += h << 15 ^ 0xffffcd7d;
        h ^= h >>> 10;
        h += h << 3;
        h ^= h >>> 6;
        h += ( h << 2 ) + ( h << 14 );
        return h ^ h >>> 16;
    }

    /**
     * @return the current project version
     */
    public static String projectVersion() {
        return cloudProperties.getProperty( "application.version" );
    }

    /**
     * @return the current project version
     */
    public static String projecBuildTimestamp() {
        return cloudProperties.getProperty( "application.build.timestamp" );
    }

    /**
     * extract first element of array(ensure there is only one element)
     * 
     * @param objects
     * @return single result
     */
    public static <T> Optional<T> singleResult(final Object[] objects) {
        if ( objects != null && objects.length > 0 ) {
            if ( objects.length != 1 )
                throw new IncorrectResultSizeDataAccessException( 1, objects.length );
            return Optional.of( (T) objects[0] );
        }
        return Optional.absent();
    }

    private SpaceUtility() {}
}
