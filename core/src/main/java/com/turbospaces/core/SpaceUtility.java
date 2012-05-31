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

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.util.ObjectUtils;

import com.esotericsoftware.kryo.serialize.EnumSerializer;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ForwardingConcurrentMap;
import com.google.common.collect.MapMaker;
import com.google.common.io.Closeables;
import com.turbospaces.api.AbstractSpaceConfiguration;
import com.turbospaces.api.JSpace;
import com.turbospaces.api.SpaceCapacityOverflowException;
import com.turbospaces.api.SpaceErrors;
import com.turbospaces.api.SpaceOperation;
import com.turbospaces.api.SpaceTopology;
import com.turbospaces.model.BO;
import com.turbospaces.model.BasicBO;
import com.turbospaces.network.MethodCall;
import com.turbospaces.network.MethodCall.BeginTransactionMethodCall;
import com.turbospaces.network.MethodCall.CommitRollbackMethodCall;
import com.turbospaces.network.MethodCall.EvictAllMethodCall;
import com.turbospaces.network.MethodCall.EvictElementsMethodCall;
import com.turbospaces.network.MethodCall.EvictPercentageMethodCall;
import com.turbospaces.network.MethodCall.FetchMethodCall;
import com.turbospaces.network.MethodCall.GetMbUsedMethodCall;
import com.turbospaces.network.MethodCall.GetSizeMethodCall;
import com.turbospaces.network.MethodCall.GetSpaceTopologyMethodCall;
import com.turbospaces.network.MethodCall.NotifyListenerMethodCall;
import com.turbospaces.network.MethodCall.WriteMethodCall;
import com.turbospaces.serialization.DecoratedKryo;
import com.turbospaces.serialization.FieldsSerializer;
import com.turbospaces.spaces.EntryKeyLockQuard;
import com.turbospaces.spaces.KeyLocker;
import com.turbospaces.spaces.tx.TransactionScopeKeyLocker;

/**
 * Space utils placeholder.
 * 
 * @since 0.1
 */
@ThreadSafe
@SuppressWarnings({ "unchecked" })
public abstract class SpaceUtility {
    private static final Logger LOGGER = LoggerFactory.getLogger( SpaceUtility.class );
    private static final Properties cloudProperties;

    static {
        LOGGER.trace( "initializing {}", SpaceUtility.class.toString() );
        ClassPathResource resource = new ClassPathResource( "turbospaces.properties" );
        cloudProperties = new Properties();
        InputStream inputStream = null;
        try {
            inputStream = resource.getInputStream();
            cloudProperties.load( inputStream );
        }
        catch ( IOException e ) {
            LOGGER.error( e.getMessage(), e );
        }
        finally {
            Closeables.closeQuietly( inputStream );
        }
    }

    /**
     * check whether template object(in form of property values) matches with actual bean's property value. This method
     * is efficiently used for {@link JSpace#notify(Object, com.turbospaces.api.SpaceNotificationListener, int)}
     * operation implementation.</p>
     * 
     * @param templatePropertyValues
     *            template object's properties
     * @param entryPropertyValues
     *            actual entry's properties
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
     * wrap given kryo configuration with space defaults and register all user supplied classes(with some sugar).</p>
     * 
     * @param configuration
     *            space configuration
     * @param kryo
     *            user custom kryo serializer
     * @throws ClassNotFoundException
     *             re-throw class registration exceptions
     * @throws NoSuchMethodException
     *             re-throw cglib exceptions
     * @throws SecurityException
     *             re-throw cglib exceptions
     * @throws IntrospectionException
     *             re-throw introspection results
     */
    @SuppressWarnings("rawtypes")
    public static void registerSpaceClasses(final AbstractSpaceConfiguration configuration,
                                            final DecoratedKryo kryo)
                                                                     throws ClassNotFoundException,
                                                                     SecurityException,
                                                                     NoSuchMethodException,
                                                                     IntrospectionException {
        kryo.register( SpaceOperation.class, new EnumSerializer( SpaceOperation.class ) );
        kryo.register( SpaceTopology.class, new EnumSerializer( SpaceTopology.class ) );

        kryo.register( MethodCall.class, new FieldsSerializer( kryo, new BasicBO( MethodCall.class ) ) );
        kryo.register( WriteMethodCall.class, new FieldsSerializer( kryo, new BasicBO( WriteMethodCall.class ) ) );
        kryo.register( FetchMethodCall.class, new FieldsSerializer( kryo, new BasicBO( FetchMethodCall.class ) ) );
        kryo.register( BeginTransactionMethodCall.class, new FieldsSerializer( kryo, new BasicBO( BeginTransactionMethodCall.class ) ) );
        kryo.register( CommitRollbackMethodCall.class, new FieldsSerializer( kryo, new BasicBO( CommitRollbackMethodCall.class ) ) );
        kryo.register( GetSpaceTopologyMethodCall.class, new FieldsSerializer( kryo, new BasicBO( GetSpaceTopologyMethodCall.class ) ) );
        kryo.register( GetMbUsedMethodCall.class, new FieldsSerializer( kryo, new BasicBO( GetMbUsedMethodCall.class ) ) );
        kryo.register( GetSizeMethodCall.class, new FieldsSerializer( kryo, new BasicBO( GetSizeMethodCall.class ) ) );
        kryo.register( NotifyListenerMethodCall.class, new FieldsSerializer( kryo, new BasicBO( NotifyListenerMethodCall.class ) ) );
        kryo.register( EvictAllMethodCall.class, new FieldsSerializer( kryo, new BasicBO( EvictAllMethodCall.class ) ) );
        kryo.register( EvictPercentageMethodCall.class, new FieldsSerializer( kryo, new BasicBO( EvictPercentageMethodCall.class ) ) );
        kryo.register( EvictElementsMethodCall.class, new FieldsSerializer( kryo, new BasicBO( EvictElementsMethodCall.class ) ) );

        Collection persistentEntities = configuration.getMappingContext().getPersistentEntities();
        BasicPersistentEntity[] persistentEntitiesAsArray = (BasicPersistentEntity[]) persistentEntities
                .toArray( new BasicPersistentEntity[persistentEntities.size()] );
        BO.registerPersistentClasses( kryo, persistentEntitiesAsArray );
    }

    /**
     * raise new {@link SpaceCapacityOverflowException} exception with given maxCapacity restriction and for object
     * which caused space overflow.
     * 
     * @param size
     *            how many items is currently in space
     * @param obj
     *            object that needs to be added to the space
     */
    public static void raiseSpaceCapacityOverflowException(final long size,
                                                           final Object obj) {
        throw new SpaceCapacityOverflowException( size, obj );
    }

    /**
     * raise new {@link DuplicateKeyException} exception for given uniqueIdentifier and persistent class.
     * 
     * @param uniqueIdentifier
     *            primary key
     * @param persistentClass
     *            space class
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
     *            primary key
     * @param persistentClass
     *            space class
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
     *            primary key
     * @param timeout
     *            lock acquire timeout
     * @param write
     *            exclusive write or exclusive read lock
     * @see SpaceErrors#UNABLE_TO_ACQUIRE_LOCK
     */
    public static void raiseCannotAcquireLockException(final Object uniqueIdentifier,
                                                       final long timeout,
                                                       final boolean write) {
        String type = write ? "write" : "exclusive read";
        throw new CannotAcquireLockException( String.format( SpaceErrors.UNABLE_TO_ACQUIRE_LOCK, type, uniqueIdentifier, timeout ) );
    }

    /**
     * wrap original key locker by applying load distribution hash function and make it more concurrent(parallel)
     * 
     * @return more concurrent parallel key locker
     */
    public static KeyLocker parallelizedKeyLocker() {
        KeyLocker locker = new KeyLocker() {
            private final KeyLocker[] segments = new KeyLocker[1 << 8];
            private int mask;

            {
                for ( int i = 0; i < segments.length; i++ )
                    segments[i] = new TransactionScopeKeyLocker();
                mask = segments.length - 1;
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

            private KeyLocker segmentFor(final Object key) {
                return segments[( JVMUtil.jdkRehash( key.hashCode() ) & Integer.MAX_VALUE ) & mask];
            }
        };
        return locker;
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
     * extract first element of array(ensure there is only one element).</p>
     * 
     * @param objects
     *            all objects
     * @return single result (if the object's size equals 1)
     * @throws IncorrectResultSizeDataAccessException
     *             if object's size not equals 1
     */
    public static <T> Optional<T> singleResult(final Object[] objects) {
        if ( objects != null && objects.length > 0 ) {
            if ( objects.length != 1 )
                throw new IncorrectResultSizeDataAccessException( 1, objects.length );
            return Optional.of( (T) objects[0] );
        }
        return Optional.absent();
    }

    /**
     * make new concurrent computation hash map(actually without guava). probably later this is subject for migration to
     * {@link MapMaker}.</p>
     * 
     * @param compFunction
     *            the computation function(callback)
     * @return concurrent map where get requests for missing keys will cause automatic creation of key-value for key
     *         using user supplied <code>compFunction</code>
     */
    public static <K, V> ConcurrentMap<K, V> newCompMap(final Function<K, V> compFunction) {
        return new ForwardingConcurrentMap<K, V>() {
            private final ConcurrentMap<K, V> delegate = new ConcurrentHashMap<K, V>();

            @Override
            protected ConcurrentMap<K, V> delegate() {
                return delegate;
            }

            @Override
            public V get(final Object key) {
                V v = super.get( key );
                if ( v == null )
                    synchronized ( delegate ) {
                        v = super.get( key );
                        if ( v == null ) {
                            v = compFunction.apply( (K) key );
                            V prev = putIfAbsent( (K) key, v );
                            if ( prev != null )
                                v = prev;
                        }
                    }
                return v;
            }
        };
    }

    private SpaceUtility() {}
}
