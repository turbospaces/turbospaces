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
package com.turbospaces.spaces;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.concurrent.ThreadSafe;

import org.jgroups.Address;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.esotericsoftware.kryo.ObjectBuffer;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.turbospaces.api.ClientSpaceConfiguration;
import com.turbospaces.api.JSpace;
import com.turbospaces.api.SpaceErrors;
import com.turbospaces.api.SpaceNotificationListener;
import com.turbospaces.api.SpaceTopology;
import com.turbospaces.core.SpaceUtility;
import com.turbospaces.model.BO;
import com.turbospaces.network.MethodCall;
import com.turbospaces.network.MethodCall.BeginTransactionMethodCall;
import com.turbospaces.network.MethodCall.CommitRollbackMethodCall;
import com.turbospaces.network.MethodCall.GetMbUsedMethodCall;
import com.turbospaces.network.MethodCall.GetSizeMethodCall;
import com.turbospaces.network.MethodCall.GetSpaceTopologyMethodCall;
import com.turbospaces.network.MethodCall.ModifyMethodCall;
import com.turbospaces.network.NetworkCommunicationDispatcher;
import com.turbospaces.pool.ObjectPool;
import com.turbospaces.spaces.tx.SpaceTransactionHolder;
import com.turbospaces.spaces.tx.TransactionModificationContextProxy;

/**
 * remote JSpace proxy which works with partitioned and synchronously replicated java spaces and implements exactly the
 * same API as {@link OffHeapJSpace} and {@link JSpace}, but remotely.
 * 
 * @since 0.1
 */
@ThreadSafe
public class RemoteJSpace implements TransactionalJSpace, InitializingBean, SpaceErrors {
    private final NetworkCommunicationDispatcher clientReceiever;
    private final ClientSpaceConfiguration configuration;

    private SpaceTopology topology;
    private RequestOptions getAllResponsesOption, getFirstResponseOption;
    private final ObjectPool<ObjectBuffer> objectBufferPool;

    /**
     * @param configuration
     */
    public RemoteJSpace(final ClientSpaceConfiguration configuration) {
        this.clientReceiever = configuration.getReceiever();
        this.configuration = configuration;
        this.objectBufferPool = SpaceUtility.newObjectBufferPool();
    }

    @Override
    public void notify(final Object template,
                       final SpaceNotificationListener listener,
                       final int modifiers) {
        final ObjectBuffer objectBuffer = borrowObjectBuffer();
        final Address[] serverNodes = clientReceiever.getServerNodes( getSpaceTopology() );
        try {
            byte[] serializedData = objectBuffer.writeClassAndObject( template );

            MethodCall.NotifyListenerMethodCall methodCall = new MethodCall.NotifyListenerMethodCall();
            methodCall.setEntity( serializedData );
            clientReceiever.sendAndReceive( methodCall, objectBuffer, serverNodes );
        }
        finally {
            objectBufferPool.returnObject( objectBuffer );
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public Object[] fetch(final Object template,
                          final long timeout,
                          final int maxResults,
                          final int modifiers) {
        final BO bo = getSpaceConfiguration().boFor( template.getClass() );
        final ObjectBuffer objectBuffer = borrowObjectBuffer();
        final SpaceTransactionHolder transactionHolder = getTransactionHolder();
        final CacheStoreEntryWrapper entryWrapper = new CacheStoreEntryWrapper( bo, getSpaceConfiguration(), template );
        final Address[] serverNodes = clientReceiever.getServerNodes();
        final boolean returnAsBytes = SpaceModifiers.isReturnAsBytes( modifiers );
        final boolean matchById = SpaceModifiers.isMatchById( modifiers );

        try {
            byte[] serializedData = objectBuffer.writeClassAndObject( entryWrapper.getBean() );

            MethodCall.FetchMethodCall methodCall = new MethodCall.FetchMethodCall();
            methodCall.setEntity( serializedData );
            methodCall.setTimeout( timeout );
            methodCall.setModifiers( modifiers );
            methodCall.setMaxResults( maxResults );

            Address[] addresses = serverNodes;
            if ( getSpaceTopology().isPartitioned() )
                /**
                 * 1. if routing field provided explicitly, you consistent hashing to determine target node
                 * 2. if no routing field is defined explicitly and matchById modifier specified, treat ID as routing
                 * 3. otherwise call needs to be broadcasted to all server nodes
                 */
                if ( entryWrapper.getRouting() != null )
                    addresses = new Address[] { determineDestination( serverNodes, entryWrapper.getRouting() ) };
                else if ( matchById )
                    addresses = new Address[] { determineDestination( serverNodes, entryWrapper.getRoutingOrId() ) };

            associateTransaction( addresses, objectBuffer, transactionHolder, methodCall );

            List response = Lists.newLinkedList();
            for ( MethodCall next : clientReceiever.sendAndReceive( methodCall, objectBuffer, addresses ) )
                if ( next.getResponseBody() != null ) {
                    byte[][] readObjectData = objectBuffer.readObjectData( next.getResponseBody(), byte[][].class );
                    for ( byte[] bytes : readObjectData ) {
                        Object obj = returnAsBytes ? ByteBuffer.wrap( bytes ) : objectBuffer.readObjectData( bytes, template.getClass() );
                        response.add( obj );
                    }
                }
            return response.toArray( returnAsBytes ? new ByteBuffer[response.size()] : new Object[response.size()] );
        }
        finally {
            objectBufferPool.returnObject( objectBuffer );
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void write(final Object entry,
                      final long timeToLive,
                      final long timeout,
                      final int modifiers) {
        Preconditions.checkNotNull( entry );
        final BO bo = getSpaceConfiguration().boFor( entry.getClass() );
        final ObjectBuffer objectBuffer = borrowObjectBuffer();
        final SpaceTransactionHolder transactionHolder = getTransactionHolder();
        final CacheStoreEntryWrapper entryWrapper = new CacheStoreEntryWrapper( bo, getSpaceConfiguration(), entry );
        final Address[] serverNodes = clientReceiever.getServerNodes();

        try {
            byte[] serializedData = objectBuffer.writeClassAndObject( entryWrapper.getBean() );

            MethodCall.WriteMethodCall methodCall = new MethodCall.WriteMethodCall();
            methodCall.setEntity( serializedData );
            methodCall.setTimeToLive( timeToLive );
            methodCall.setTimeout( timeout );
            methodCall.setModifiers( modifiers );

            Address[] addresses = serverNodes;
            if ( getSpaceTopology().isPartitioned() )
                addresses = new Address[] { determineDestination( serverNodes, entryWrapper.getRoutingOrId() ) };
            associateTransaction( addresses, objectBuffer, transactionHolder, methodCall );
            clientReceiever.sendAndReceive( methodCall, objectBuffer, addresses );
        }
        finally {
            objectBufferPool.returnObject( objectBuffer );
        }
    }

    @Override
    public long size() {
        long size = 0;

        Address[] addresses = clientReceiever.getServerNodes( getSpaceTopology() );
        ObjectBuffer objectBuffer = borrowObjectBuffer();
        GetSizeMethodCall methodCall = new GetSizeMethodCall();

        try {
            for ( MethodCall object : clientReceiever.sendAndReceive( methodCall, objectBuffer, addresses ) )
                size += objectBuffer.readObjectData( object.getResponseBody(), Long.class );
        }
        finally {
            objectBufferPool.returnObject( objectBuffer );
        }

        return size;
    }

    @Override
    public int mbUsed() {
        int mdUbed = 0;

        Address[] addresses = clientReceiever.getServerNodes();
        ObjectBuffer objectBuffer = borrowObjectBuffer();
        GetMbUsedMethodCall methodCall = new GetMbUsedMethodCall();

        try {
            for ( MethodCall object : clientReceiever.sendAndReceive( methodCall, objectBuffer, addresses ) )
                mdUbed += objectBuffer.readObjectData( object.getResponseBody(), Integer.class );
        }
        finally {
            objectBufferPool.returnObject( objectBuffer );
        }

        return mdUbed;
    }

    @Override
    public SpaceTopology getSpaceTopology() {
        if ( topology == null ) {
            ObjectBuffer objectBuffer = borrowObjectBuffer();
            GetSpaceTopologyMethodCall methodCall = new GetSpaceTopologyMethodCall();

            try {
                topology = objectBuffer.readObjectData( clientReceiever.sendAndReceive(
                        methodCall,
                        objectBuffer,
                        clientReceiever.getServerNodes( SpaceTopology.PARTITIONED ) )[0].getResponseBody(), SpaceTopology.class );
            }
            finally {
                objectBufferPool.returnObject( objectBuffer );
            }
        }

        return topology;
    }

    @Override
    public ClientSpaceConfiguration getSpaceConfiguration() {
        return configuration;
    }

    @Override
    public void afterPropertiesSet() {
        getAllResponsesOption = new RequestOptions( ResponseMode.GET_ALL, configuration.getDefaultCommunicationTimeoutInMillis() );
        getFirstResponseOption = new RequestOptions( ResponseMode.GET_FIRST, configuration.getDefaultCommunicationTimeoutInMillis() );

        getAllResponsesOption.setAnycasting( true );
        getFirstResponseOption.setAnycasting( true );
    }

    @Override
    public void destroy() {}

    @Override
    public void syncTx(final Object ctx,
                       final boolean commit) {
        ObjectBuffer objectBuffer = borrowObjectBuffer();
        try {
            TransactionModificationContextProxy c = (TransactionModificationContextProxy) ctx;
            Map<Address, Long> transactionIds = c.getTransactionIds();
            for ( Entry<Address, Long> entry : transactionIds.entrySet() ) {
                Address address = entry.getKey();
                Long transactionId = entry.getValue();

                CommitRollbackMethodCall methodCall = new CommitRollbackMethodCall( commit );
                methodCall.setTransactionId( transactionId );
                clientReceiever.sendAndReceive( methodCall, objectBuffer, address );
            }
        }
        finally {
            objectBufferPool.returnObject( objectBuffer );
        }
    }

    @Override
    public SpaceTransactionHolder getTransactionHolder() {
        return (SpaceTransactionHolder) TransactionSynchronizationManager.getResource( this );
    }

    @Override
    public void bindTransactionHolder(final SpaceTransactionHolder transactionHolder) {
        TransactionSynchronizationManager.bindResource( this, transactionHolder );
    }

    @Override
    public Object unbindTransactionHolder(final SpaceTransactionHolder transactionHolder) {
        return TransactionSynchronizationManager.unbindResource( this );
    }

    private void propagateTransactionIfNecessary(final Address address,
                                                 final ObjectBuffer objectBuffer) {
        SpaceTransactionHolder transactionHolder = getTransactionHolder();
        if ( transactionHolder != null ) {
            TransactionModificationContextProxy mc = (TransactionModificationContextProxy) transactionHolder.getModificationContext();
            boolean hasAssignedId = mc.hasAssignedId( address );
            if ( !hasAssignedId ) {
                BeginTransactionMethodCall methodCall = new BeginTransactionMethodCall();
                methodCall.setTransactionTimeout( transactionHolder.getTimeToLiveInMillis() );

                byte[] bytes = clientReceiever.sendAndReceive( methodCall, objectBuffer, address )[0].getResponseBody();
                Long transactionId = objectBuffer.readObjectData( bytes, Long.class );
                mc.assignTransactionId( address, transactionId );
            }
        }
    }

    private void associateTransaction(final Address[] serverNodes,
                                      final ObjectBuffer borrowObject,
                                      final SpaceTransactionHolder transactionHolder,
                                      final ModifyMethodCall methodCall) {
        for ( Address address : serverNodes ) {
            propagateTransactionIfNecessary( address, borrowObject );
            setTransactionIdIfNecessary( transactionHolder, methodCall, address );
        }
    }

    private static void setTransactionIdIfNecessary(final SpaceTransactionHolder transactionHolder,
                                                    final ModifyMethodCall methodCall,
                                                    final Address address) {
        if ( transactionHolder != null && transactionHolder.getModificationContext() != null )
            methodCall.setTransactionId( ( (TransactionModificationContextProxy) transactionHolder.getModificationContext() )
                    .getAssignedTransactionId( address ) );
    }

    private static Address determineDestination(final Address[] addresses,
                                                final Object key) {
        int index = ( key.hashCode() & Integer.MAX_VALUE ) % addresses.length;
        return addresses[index];
    }

    private ObjectBuffer borrowObjectBuffer() {
        ObjectBuffer borrowObject = objectBufferPool.borrowObject();
        borrowObject.setKryo( configuration.getKryo() );
        return borrowObject;
    }
}
