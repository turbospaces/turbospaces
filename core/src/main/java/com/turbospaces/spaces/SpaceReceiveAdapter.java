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
package com.turbospaces.spaces;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.remoting.RemoteConnectFailureException;

import com.esotericsoftware.kryo.Kryo.RegisteredClass;
import com.esotericsoftware.kryo.ObjectBuffer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.turbospaces.api.AbstractSpaceConfiguration;
import com.turbospaces.api.JSpace;
import com.turbospaces.api.SpaceNotificationListener;
import com.turbospaces.api.SpaceOperation;
import com.turbospaces.network.MethodCall;
import com.turbospaces.network.MethodCall.BeginTransactionMethodCall;
import com.turbospaces.network.MethodCall.CommitRollbackMethodCall;
import com.turbospaces.network.MethodCall.EvictElementsMethodCall;
import com.turbospaces.network.MethodCall.EvictPercentageMethodCall;
import com.turbospaces.network.MethodCall.FetchMethodCall;
import com.turbospaces.network.MethodCall.NotifyListenerMethodCall;
import com.turbospaces.network.MethodCall.WriteMethodCall;
import com.turbospaces.spaces.tx.SpaceTransactionHolder;
import com.turbospaces.spaces.tx.TransactionModificationContext;

/**
 * jspace network communication adapter
 * 
 * @since 0.1
 */
@ThreadSafe
class SpaceReceiveAdapter extends ReceiverAdapter implements InitializingBean, DisposableBean {
    private final Logger logger = LoggerFactory.getLogger( getClass() );

    private final ConcurrentHashMap<Address, Cache<Long, SpaceTransactionHolder>> durableTransactions;
    private final AbstractJSpace jSpace;
    private ScheduledFuture<?> cleaupFuture;
    private volatile Address[] clientConnectors;

    SpaceReceiveAdapter(final AbstractJSpace jSpace) {
        this.jSpace = jSpace;
        this.durableTransactions = new ConcurrentHashMap<Address, Cache<Long, SpaceTransactionHolder>>();
    }

    @Override
    public void afterPropertiesSet() {
        ScheduledExecutorService scheduledExecutorService = jSpace.getSpaceConfiguration().getScheduledExecutorService();
        cleaupFuture = scheduledExecutorService.scheduleWithFixedDelay( new Runnable() {
            @Override
            public void run() {
                if ( !durableTransactions.isEmpty() ) {
                    Collection<Cache<Long, SpaceTransactionHolder>> values = durableTransactions.values();
                    for ( Cache<Long, SpaceTransactionHolder> cache : values ) {
                        logger.debug( "running automatic cleanup of dead transaction for {}", cache );
                        cache.cleanUp();
                    }
                }
            }
        }, 0, jSpace.getSpaceConfiguration().getCacheCleanupPeriod(), TimeUnit.MILLISECONDS );
    }

    @Override
    public void destroy() {
        if ( cleaupFuture != null )
            cleaupFuture.cancel( false );
    }

    @Override
    public void receive(final Message msg) {
        final byte[] data = msg.getBuffer();
        final ObjectBuffer objectBuffer = new ObjectBuffer( jSpace.getSpaceConfiguration().getKryo() );
        final Address nodeRaised = msg.getSrc();

        final MethodCall methodCall = (MethodCall) objectBuffer.readClassAndObject( data );
        final short id = methodCall.getMethodId();

        SpaceMethodsMapping spaceMethodsMapping = SpaceMethodsMapping.values()[methodCall.getMethodId()];
        logger.debug( "received MethodCall[{}] from {}", spaceMethodsMapping, nodeRaised );

        if ( id == SpaceMethodsMapping.BEGIN_TRANSACTION.ordinal() )
            sendResponseBackAfterExecution( methodCall, new Runnable() {
                @Override
                public void run() {
                    /**
                     * 1. read transaction timeout
                     * 2. create local durable transaction for remote client and assign id
                     * 3. propagate remote transaction timeout and apply to local transaction
                     * 4. send transaction id back to client
                     */
                    BeginTransactionMethodCall beginTransactionMethodCall = (BeginTransactionMethodCall) methodCall;
                    long transactionTimeout = beginTransactionMethodCall.getTransactionTimeout();
                    SpaceTransactionHolder spaceTransactionHolder = new SpaceTransactionHolder();
                    spaceTransactionHolder.setSynchronizedWithTransaction( true );
                    spaceTransactionHolder.setTimeoutInMillis( transactionTimeout );
                    TransactionModificationContext mc = new TransactionModificationContext();
                    mc.setProxyMode( true );
                    spaceTransactionHolder.setModificationContext( mc );
                    modificationContextFor( nodeRaised ).put( mc.getTransactionId(), spaceTransactionHolder );
                    methodCall.setResponseBody( objectBuffer.writeObjectData( mc.getTransactionId() ) );
                }
            }, nodeRaised, objectBuffer );
        else if ( id == SpaceMethodsMapping.COMMIT_TRANSACTION.ordinal() )
            sendResponseBackAfterExecution( methodCall, new Runnable() {
                @Override
                public void run() {
                    CommitRollbackMethodCall commitMethodCall = (CommitRollbackMethodCall) methodCall;
                    try {
                        SpaceTransactionHolder th = modificationContextFor( nodeRaised ).getIfPresent( commitMethodCall.getTransactionId() );
                        Preconditions.checkState(
                                th != null,
                                "unable to find transaction with id = %s for commit",
                                commitMethodCall.getTransactionId() );
                        jSpace.syncTx( th.getModificationContext(), true );
                    }
                    finally {
                        durableTransactions.remove( commitMethodCall.getTransactionId() );
                    }
                }
            },
                    nodeRaised,
                    objectBuffer );
        else if ( id == SpaceMethodsMapping.ROLLBACK_TRANSACTION.ordinal() )
            sendResponseBackAfterExecution( methodCall, new Runnable() {
                @Override
                public void run() {
                    CommitRollbackMethodCall commitMethodCall = (CommitRollbackMethodCall) methodCall;
                    try {
                        SpaceTransactionHolder th = modificationContextFor( nodeRaised ).getIfPresent( commitMethodCall.getTransactionId() );
                        Preconditions.checkState(
                                th != null,
                                "unable to find transaction with id = %s for rollback",
                                commitMethodCall.getTransactionId() );
                        jSpace.syncTx( th.getModificationContext(), false );
                    }
                    finally {
                        durableTransactions.remove( commitMethodCall.getTransactionId() );
                    }
                }
            },
                    nodeRaised,
                    objectBuffer );
        else if ( id == SpaceMethodsMapping.WRITE.ordinal() )
            sendResponseBackAfterExecution( methodCall, new Runnable() {
                @Override
                public void run() {
                    WriteMethodCall writeMethodCall = (WriteMethodCall) methodCall;
                    byte[] entityAndClassData = writeMethodCall.getEntity();
                    ByteBuffer byteBuffer = ByteBuffer.wrap( entityAndClassData );

                    int modifiers = writeMethodCall.getModifiers();
                    int timeout = writeMethodCall.getTimeout();
                    int timeToLive = writeMethodCall.getTimeToLive();

                    /**
                     * 1. read registered class (without actual data)
                     * 2. get the actual type of the remote entry
                     * 3. copy serialized entry state from the byte buffer, omit redundant serialization later
                     * 4. find appropriate transaction modification context if any
                     * 5. call write method itself
                     */
                    RegisteredClass entryClass = jSpace.getSpaceConfiguration().getKryo().readClass( byteBuffer );
                    Class<?> entryType = entryClass.getType();
                    byte[] entityData = Arrays.copyOfRange( byteBuffer.array(), byteBuffer.position(), byteBuffer.capacity() );
                    Object entry = jSpace.getSpaceConfiguration().getKryo().readObjectData( byteBuffer, entryType );

                    SpaceTransactionHolder holder = null;
                    if ( writeMethodCall.getTransactionId() != 0 )
                        holder = modificationContextFor( nodeRaised ).getIfPresent( writeMethodCall.getTransactionId() );

                    jSpace.write( holder, entry, entityData, timeToLive, timeout, modifiers );
                    writeMethodCall.reset();
                }
            }, nodeRaised, objectBuffer );
        else if ( id == SpaceMethodsMapping.FETCH.ordinal() )
            sendResponseBackAfterExecution( methodCall, new Runnable() {
                @Override
                public void run() {
                    FetchMethodCall fetchMethodCall = (FetchMethodCall) methodCall;
                    byte[] entityData = fetchMethodCall.getEntity();

                    int originalModifiers = fetchMethodCall.getModifiers();
                    int timeout = fetchMethodCall.getTimeout();
                    int maxResults = fetchMethodCall.getMaxResults();
                    Object template = objectBuffer.readClassAndObject( entityData );
                    int modifiers = originalModifiers | JSpace.RETURN_AS_BYTES;

                    SpaceTransactionHolder holder = null;
                    if ( fetchMethodCall.getTransactionId() != 0 )
                        holder = modificationContextFor( nodeRaised ).getIfPresent( fetchMethodCall.getTransactionId() );

                    ByteBuffer[] buffers = (ByteBuffer[]) jSpace.fetch( holder, template, timeout, maxResults, modifiers );
                    if ( buffers != null ) {
                        byte[][] response = new byte[buffers.length][];
                        for ( int i = 0; i < buffers.length; i++ ) {
                            ByteBuffer buffer = buffers[i];
                            response[i] = buffer.array();
                        }
                        fetchMethodCall.setResponseBody( objectBuffer.writeObjectData( response ) );
                    }
                    fetchMethodCall.reset();
                }
            }, nodeRaised, objectBuffer );
        else if ( id == SpaceMethodsMapping.NOTIFY.ordinal() )
            sendResponseBackAfterExecution( methodCall, new Runnable() {
                @Override
                public void run() {
                    NotifyListenerMethodCall registerMethodCall = (NotifyListenerMethodCall) methodCall;
                    byte[] entityData = registerMethodCall.getEntity();

                    int originalModifiers = registerMethodCall.getModifiers();
                    Object template = objectBuffer.readClassAndObject( entityData );
                    int modifiers = originalModifiers | JSpace.RETURN_AS_BYTES;
                    jSpace.notify( template, new SpaceNotificationListener() {

                        @Override
                        public void handleNotification(final Object entity,
                                                       final SpaceOperation operation) {
                            ObjectBuffer innerObjectBuffer = new ObjectBuffer( jSpace.getSpaceConfiguration().getKryo() );
                            sendResponseBackAfterExecution( methodCall, new Runnable() {
                                @Override
                                public void run() {
                                    NotifyListenerMethodCall methodCall = new NotifyListenerMethodCall();
                                    methodCall.setEntity( ( (ByteBuffer) entity ).array() );
                                    methodCall.setOperation( operation );
                                }
                            }, nodeRaised, innerObjectBuffer );
                        }
                    }, modifiers );
                }
            }, nodeRaised, objectBuffer );
        else if ( id == SpaceMethodsMapping.SIZE.ordinal() )
            sendResponseBackAfterExecution( methodCall, new Runnable() {
                @Override
                public void run() {
                    methodCall.setResponseBody( objectBuffer.writeObjectData( jSpace.size() ) );
                }
            }, nodeRaised, objectBuffer );
        else if ( id == SpaceMethodsMapping.MB_USED.ordinal() )
            sendResponseBackAfterExecution( methodCall, new Runnable() {
                @Override
                public void run() {
                    methodCall.setResponseBody( objectBuffer.writeObjectData( jSpace.mbUsed() ) );
                }
            }, nodeRaised, objectBuffer );
        else if ( id == SpaceMethodsMapping.EVICT_ELEMENTS.ordinal() )
            sendResponseBackAfterExecution( methodCall, new Runnable() {
                @Override
                public void run() {
                    EvictElementsMethodCall evictElementsMethodCall = (EvictElementsMethodCall) methodCall;
                    methodCall.setResponseBody( objectBuffer.writeObjectData( jSpace.evictElements( evictElementsMethodCall.getElements() ) ) );
                }
            }, nodeRaised, objectBuffer );
        else if ( id == SpaceMethodsMapping.EVICT_PERCENTAGE.ordinal() )
            sendResponseBackAfterExecution( methodCall, new Runnable() {
                @Override
                public void run() {
                    EvictPercentageMethodCall evictPercentageMethodCall = (EvictPercentageMethodCall) methodCall;
                    methodCall.setResponseBody( objectBuffer.writeObjectData( jSpace.evictPercentage( evictPercentageMethodCall.getPercentage() ) ) );
                }
            },
                    nodeRaised,
                    objectBuffer );
        else if ( id == SpaceMethodsMapping.EVICT_ALL.ordinal() )
            sendResponseBackAfterExecution( methodCall, new Runnable() {
                @Override
                public void run() {
                    methodCall.setResponseBody( objectBuffer.writeObjectData( jSpace.evictAll() ) );
                }
            }, nodeRaised, objectBuffer );
        else if ( id == SpaceMethodsMapping.SPACE_TOPOLOGY.ordinal() )
            sendResponseBackAfterExecution( methodCall, new Runnable() {
                @Override
                public void run() {
                    methodCall.setResponseBody( objectBuffer.writeObjectData( jSpace.getSpaceConfiguration().getTopology() ) );
                }
            }, nodeRaised, objectBuffer );
    }

    private void sendResponseBackAfterExecution(final MethodCall methodCall,
                                                final Runnable task,
                                                final Address address,
                                                final ObjectBuffer objectBuffer)
                                                                                throws RemoteConnectFailureException {
        try {
            task.run();
        }
        catch ( RuntimeException ex ) {
            methodCall.setException( ex );
            logger.error( ex.getMessage(), ex );
            Throwables.propagate( ex );
        }
        finally {
            JChannel jChannel = jSpace.getSpaceConfiguration().getJChannel();
            Message messageBack = new Message();
            messageBack.setBuffer( objectBuffer.writeClassAndObject( methodCall ) );
            messageBack.setDest( address );
            messageBack.setSrc( jChannel.getAddress() );

            try {
                jChannel.send( messageBack );
            }
            catch ( Exception e ) {
                logger.error( e.getMessage(), e );
                throw new RemoteConnectFailureException( "unable to send response back to " + address, e );
            }
        }
    }

    @Override
    public void suspect(final Address mbr) {
        Cache<Long, SpaceTransactionHolder> modificationContexts = modificationContextFor( mbr );
        Set<Long> keys = modificationContexts.asMap().keySet();
        if ( keys.size() > 0 )
            logger.warn( "Address {} has been suspected, this may cause automatic rollback for active transactions soon, ids = {}", mbr, keys );
    }

    @Override
    public void viewAccepted(final View view) {
        final Address[] prevConnectors = clientConnectors;
        final Address[] newClientConnectors = getClientConnections( view );

        if ( prevConnectors != null ) {
            List<Address> l = new ArrayList<Address>( Arrays.asList( prevConnectors ) );
            l.removeAll( Arrays.asList( newClientConnectors ) );

            // automatically roll-back un-committed transactions, we can't leave them as this as they holds locks
            for ( Address a : l ) {
                Cache<Long, SpaceTransactionHolder> modificationContextFor = modificationContextFor( a );
                ConcurrentMap<Long, SpaceTransactionHolder> asMap = modificationContextFor.asMap();
                Set<Entry<Long, SpaceTransactionHolder>> entrySet = asMap.entrySet();
                for ( Entry<Long, SpaceTransactionHolder> entry : entrySet ) {
                    Long transactionId = entry.getKey();
                    SpaceTransactionHolder transactionHolder = entry.getValue();
                    logger.warn( "automatically rolling back transaction id={} due to client's connection disconnect", transactionId );
                    jSpace.syncTx( transactionHolder.getModificationContext(), false );
                    modificationContextFor.invalidate( transactionId );
                }
            }
        }

        clientConnectors = newClientConnectors;
    }

    @VisibleForTesting
    Cache<Long, SpaceTransactionHolder> modificationContextFor(final Address address) {
        Cache<Long, SpaceTransactionHolder> cache = durableTransactions.get( address );
        if ( cache == null )
            synchronized ( this ) {
                CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
                applyExpireAfterWriteSettings( cacheBuilder );
                cache = cacheBuilder.removalListener( new RemovalListener<Long, SpaceTransactionHolder>() {
                    @Override
                    public void onRemoval(final RemovalNotification<Long, SpaceTransactionHolder> notification) {
                        Long transactionId = notification.getKey();
                        SpaceTransactionHolder txHolder = notification.getValue();

                        if ( notification.wasEvicted() ) {
                            logger.warn( "cleaning up(rolling back) expired transaction id={}", transactionId );
                            jSpace.syncTx( txHolder.getModificationContext(), false );
                        }
                    }
                } ).build();
                durableTransactions.putIfAbsent( address, cache );
                cache = durableTransactions.get( address );
            }
        return cache;
    }

    @VisibleForTesting
    void applyExpireAfterWriteSettings(final CacheBuilder<Object, Object> builder) {
        builder.expireAfterWrite( AbstractSpaceConfiguration.defaultTransactionTimeout(), TimeUnit.SECONDS );
    }

    @VisibleForTesting
    Address[] getClientConnections(@SuppressWarnings("unused") final View view) {
        return jSpace.getSpaceConfiguration().getMessageDispatcher().getRawClientNodes();
    }
}
