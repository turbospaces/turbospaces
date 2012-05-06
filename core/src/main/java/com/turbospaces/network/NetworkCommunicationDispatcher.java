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
package com.turbospaces.network;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

import org.jgroups.Address;
import org.jgroups.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.remoting.RemoteConnectFailureException;
import org.springframework.remoting.RemoteInvocationFailureException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import com.google.common.annotations.VisibleForTesting;
import com.turbospaces.api.AbstractSpaceConfiguration;
import com.turbospaces.api.SpaceErrors;
import com.turbospaces.core.SimpleRequestResponseCorrelator;

/**
 * central point for network communication over jgroups(minimalistic version instead of jgroups's high-level building
 * blocks for better performance and lower GC garbage allocations).
 * 
 * @since 0.1
 */
@ThreadSafe
public final class NetworkCommunicationDispatcher extends ServerCommunicationDispatcher implements SpaceErrors {
    private final Logger logger = LoggerFactory.getLogger( getClass() );
    private final AtomicLong correlationIds = new AtomicLong();
    private Kryo kryo;
    private final SimpleRequestResponseCorrelator<Long, MethodCall> requestResponseCorrelator = new SimpleRequestResponseCorrelator<Long, MethodCall>();

    /**
     * create client side receiver over jchannel with given default network communication timeout.
     * 
     * @param configuration
     */
    public NetworkCommunicationDispatcher(final AbstractSpaceConfiguration configuration) {
        super( configuration );
    }

    /**
     * set kryo serializer
     * 
     * @param kryo
     */
    public void setKryo(final Kryo kryo) {
        this.kryo = kryo;
    }

    /**
     * send message(in form of {@link MethodCall}) to the remote destination with assigned correlation id.
     * 
     * @param destinations
     * @param methodCall
     * @param objectBuffer
     * @return original methodCall (pre-populated with correlation id)
     */
    public MethodCall[] sendAndReceive(final MethodCall methodCall,
                                       final ObjectBuffer objectBuffer,
                                       final Address... destinations) {
        int size = destinations.length;
        MethodCall[] result = new MethodCall[size];
        Long[] ids = new Long[size];
        Object[] monitors = new Object[size];
        Message[] messages = new Message[size];

        for ( int i = 0; i < size; i++ ) {
            Long correlationId = correlationIds.incrementAndGet();
            methodCall.setCorrelationId( correlationId.longValue() );

            Message message = new Message();
            message.setSrc( configuration.getJChannel().getAddress() );
            message.setDest( destinations[i] );
            message.setBuffer( objectBuffer.writeClassAndObject( methodCall ) );

            try {
                monitors[i] = requestResponseCorrelator.put( correlationId, null );
                ids[i] = correlationId;
                messages[i] = message;
                configuration.getJChannel().send( message );
                if ( logger.isDebugEnabled() )
                    logger.debug( "sent Jgroups message with correlation_id={} and destinations = {}", correlationId, destinations );
            }
            catch ( Exception t ) {
                requestResponseCorrelator.clear( correlationId );
                throw new RemoteConnectFailureException( "unable to send message to " + destinations[i], t );
            }
        }

        // now collect all results in synchronous manner
        for ( int i = 0; i < size; i++ ) {
            Long id = ids[i];
            Object monitor = monitors[i];
            result[i] = requestResponseCorrelator.responseFor( id, monitor, configuration.getDefaultCommunicationTimeoutInMillis() );
        }

        return verifyNoExceptions( result, messages, methodCall );
    }

    @Override
    public void receive(final Message msg) {
        ObjectBuffer objectBuffer = objectBufferPool.borrowObject();
        objectBuffer.setKryo( kryo );

        try {
            final byte[] data = msg.getBuffer();
            final MethodCall response = (MethodCall) objectBuffer.readClassAndObject( data );
            long correlationId = response.getCorrelationId();

            requestResponseCorrelator.put( correlationId, response );
        }
        finally {
            objectBufferPool.returnObject( objectBuffer );
        }
    }

    /**
     * check each particular response and verify that there are no remote exceptions, otherwise thrown proper combined
     * exception.
     * 
     * @param result
     *            remote execution result
     * @param messages
     *            remote messages that have been sent to server node
     * @param methodCall
     *            original method call
     * 
     * @return result itself
     */
    @VisibleForTesting
    MethodCall[] verifyNoExceptions(final MethodCall[] result,
                                    final Message[] messages,
                                    final MethodCall methodCall) {
        int size = result.length;
        List<RemoteInvocationFailureException> remoteExceptions = null;
        for ( int i = 0; i < size; i++ ) {
            Message message = messages[i];
            Address destination = message.getDest();
            if ( result[i] != null ) {
                String exceptionAsString = result[i].exceptionAsString;
                if ( exceptionAsString != null ) {
                    RemoteInvocationFailureException ex = new RemoteInvocationFailureException( String.format(
                            "failed to execute %s on %s",
                            methodCall,
                            destination ), null );
                    ex.fillInStackTrace();
                    logger.error( ex.getMessage(), ex );

                    if ( remoteExceptions == null )
                        remoteExceptions = new ArrayList<RemoteInvocationFailureException>();
                    remoteExceptions.add( ex );
                }
            }
        }

        if ( remoteExceptions != null ) {
            // TODO: throw
        }
        return result;
    }
}
