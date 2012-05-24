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
package com.turbospaces.api;

import org.jgroups.util.UUID;

import com.turbospaces.network.NetworkCommunicationDispatcher;

/**
 * this is client side jspace configuration. please refer to {@link SpaceConfiguration} java-docs to get full
 * documentation.
 * 
 * @since 0.1
 */
public class ClientSpaceConfiguration extends AbstractSpaceConfiguration {
    private NetworkCommunicationDispatcher dispatcher;

    @Override
    public void afterPropertiesSet()
                                    throws Exception {
        super.afterPropertiesSet();

        getJChannel().setName( JSpace.SC + "-" + UUID.randomUUID().toString() );
        dispatcher = new NetworkCommunicationDispatcher( this );
        dispatcher.setKryo( getKryo() );
        getJChannel().setReceiver( dispatcher );
        dumpConfiguration();
    }

    /**
     * @return client side message receiver (in jgroups terminology)
     */
    public NetworkCommunicationDispatcher getReceiever() {
        return dispatcher;
    }
}
