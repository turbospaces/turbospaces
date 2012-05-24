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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.mapping.context.AbstractMappingContext;

import com.google.common.base.Preconditions;
import com.turbospaces.network.ServerCommunicationDispatcher;
import com.turbospaces.network.SpaceNetworkServiceProvider;
import com.turbospaces.serialization.DecoratedKryo;
import com.turbospaces.spaces.SimplisticJSpace;

/**
 * This is the server side {@link JSpace} configuration placeholder. Important consideration is that turbospaces relies
 * on <strong>spring-data</strong> abstraction, because in real world scenarios you would configure external data source
 * and will store updated/written entities in asynchronous (write-behind) manner - meaning that there is always backend
 * store configured and this factor is driving factor within jspace cloud architecture.</p>
 * 
 * In order to dramatically reduce efforts needed for JPA/mongoDB/hadoop/gemfire/redis/riak/couchdb/neo4j integration
 * be ready to see spring-data libraries as dependency of your project which is assumed not a big problem.</p>
 * 
 * This configuration has nothing to do with actual external data source, but something still required from you:
 * <ul>
 * <li>(<b>required step</b>) set actual mappingContext - see {@link #setMappingContext(AbstractMappingContext)}</li>
 * <li>(<b>optional step</b>) set custom kryo serializer - see {@link #setKryo(DecoratedKryo)}</li>
 * </ul>
 * 
 * <p>
 * Also this is spring-ready bean and uses standard initialization-destroy mechanism via {@link InitializingBean} and
 * {@link DisposableBean}. Just in case you want to use configuration outside spring IOC, still you can call
 * {@link #afterPropertiesSet()} and {@link #destroy()} methods manually.
 * 
 * @see JSpace
 * @see SimplisticJSpace
 * @see AbstractSpaceConfiguration
 * @since 0.1
 */
public final class SpaceConfiguration extends AbstractSpaceConfiguration {
    private SpaceNetworkServiceProvider networkServiceProvider;
    private SpaceExpirationListener<?, ?> expirationListener = new SpaceExpirationListener<Object, Object>( false ) {
        private final Logger logger = LoggerFactory.getLogger( getClass() );

        @SuppressWarnings("rawtypes")
        @Override
        public void handleNotification(final Object entity,
                                       final Object id,
                                       final Class persistentClass,
                                       final int originalTimeToLive) {
            logger.info( "entity {} with ID={} has been expired after {} milliseconds and automatically removed from space", new Object[] {
                    persistentClass.getSimpleName(), id, originalTimeToLive } );
        }
    };
    private CapacityRestriction capacityRestriction = new CapacityRestriction();
    private ServerCommunicationDispatcher dispatcher;

    /**
     * specify the space topology. either synch-replicated or partitioned. the default one is
     * {@link SpaceTopology#SYNC_REPLICATED} because partitioned one requires some configuration and understanding, so
     * you would need to think little bit about nature of the data and the rules for data partitioning before using
     * {@link SpaceTopology#PARTITIONED}. </p>
     * 
     * @param topology
     *            space topology to be used:replicated or partitioned?
     */
    public void setTopology(final SpaceTopology topology) {
        this.topology = topology;
    }

    /**
     * associate expiration listener with this jspace. this is optional step
     * 
     * @param expirationListener
     *            space expiration listener
     */
    public void setExpirationListener(final SpaceExpirationListener<?, ?> expirationListener) {
        Preconditions.checkNotNull( expirationListener );
        this.expirationListener = expirationListener;
    }

    /**
     * @return jspace capacity restriction
     */
    public CapacityRestriction getCapacityRestriction() {
        return capacityRestriction;
    }

    /**
     * @return user expiration listener(if any)
     */
    public SpaceExpirationListener<?, ?> getExpirationListener() {
        return expirationListener;
    }

    /**
     * globally restrict jspace capacity.
     * 
     * @param capacityRestriction
     *            new global space capacity restriction
     */
    public void setCapacityRestriction(final CapacityRestriction capacityRestriction) {
        this.capacityRestriction = Preconditions.checkNotNull( capacityRestriction );
    }

    @Override
    public void afterPropertiesSet()
                                    throws Exception {
        super.afterPropertiesSet();
        getJChannel().setName( JSpace.SSC + "-" + UUID.randomUUID().toString() );
        dispatcher = new ServerCommunicationDispatcher( this );
        getJChannel().setReceiver( dispatcher );
        networkServiceProvider = new SpaceNetworkServiceProvider( getJChannel(), true );
        dumpConfiguration();
    }

    /**
     * @return high-level service provider which is responsible for building 'over-jspace' communications.
     */
    public SpaceNetworkServiceProvider getNetworkServiceProvider() {
        return networkServiceProvider;
    }
}
