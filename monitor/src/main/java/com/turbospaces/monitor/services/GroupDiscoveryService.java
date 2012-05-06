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
package com.turbospaces.monitor.services;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.jgroups.JChannel;
import org.jgroups.stack.ProtocolStack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

import com.turbospaces.network.NetworkCommunicationDispatcher;

/**
 * responsible for managing jgroups connections
 * 
 * @since 0.1
 */
@SuppressWarnings("javadoc")
public class GroupDiscoveryService {
    private final Logger logger = LoggerFactory.getLogger( getClass() );
    private final Map<String, JChannel> channels = new HashMap<String, JChannel>();

    public synchronized JChannel connect(final String group,
                                         final byte[] bytes)
                                                            throws Exception {
        JChannel jChannel = channels.get( group );
        if ( jChannel == null || bytes != null ) {
            ClassPathResource resource = new ClassPathResource( "udp-largecluster.xml" );
            InputStream inputStream = resource.getInputStream();
            jChannel = new JChannel( inputStream );
            inputStream.close();
            if ( bytes != null ) {
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( bytes );
                jChannel = new JChannel( byteArrayInputStream );
                byteArrayInputStream.close();
            }
            jChannel.setDiscardOwnMessages( true );
            ProtocolStack protocolStack = jChannel.getProtocolStack();
            protocolStack.getTransport().setValue( "enable_diagnostics", false );
            jChannel.setReceiver( new NetworkCommunicationDispatcher( jChannel, 0 ) );
            logger.info( "joining network group {}", group );
            jChannel.connect( group );
            channels.put( group, jChannel );
        }

        return jChannel;
    }
}
