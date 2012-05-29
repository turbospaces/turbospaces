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

import java.io.InputStream;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.jgroups.JChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.mapping.context.AbstractMappingContext;
import org.springframework.data.mapping.model.BasicPersistentEntity;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.turbospaces.core.EffectiveMemoryManager;
import com.turbospaces.core.SpaceUtility;
import com.turbospaces.core.UnsafeMemoryManager;
import com.turbospaces.model.BO;
import com.turbospaces.network.ServerCommunicationDispatcher;
import com.turbospaces.serialization.DecoratedKryo;
import com.turbospaces.spaces.tx.SpaceTransactionManager;

/**
 * abstract jspace configuration suitable for both client/server node configuration.
 * 
 * @see SpaceConfiguration
 * @see ClientSpaceConfiguration
 * @since 0.1
 */
@SuppressWarnings("rawtypes")
public abstract class AbstractSpaceConfiguration implements ApplicationContextAware, DisposableBean, InitializingBean, SpaceErrors {
    /**
     * the maximum possible number of nodes in cluster.
     */
    public static final int MAX_CLUSTER_NODE = 1 << 10;
    private static final int DEFAULT_TRANSACTION_TIMEOUT = (int) TimeUnit.SECONDS.toSeconds( 10 );
    private static final long DEFAULT_COMMUNICATION_TIMEOUT = TimeUnit.SECONDS.toMillis( 5 );
    private static final long DEFAULT_CACHE_CLEANUP_PERIOD = TimeUnit.SECONDS.toMillis( 1 );

    private final Logger logger = LoggerFactory.getLogger( getClass() );
    /**
     * spring application context reference (if inside IOC container, not standalone)
     */
    private ApplicationContext applicationContext;
    /**
     * space logical name
     * 
     * @see #setGroup(String)
     */
    private String group = defaultGroupName();
    /**
     * POJO mapping context(mongoDB, JPA, JDBC, etc)
     * 
     * @see #setMappingContext(AbstractMappingContext)
     */
    private AbstractMappingContext mappingContext;
    /**
     * jgroups communication channel
     * 
     * @see #setjChannel(JChannel)
     */
    private JChannel jChannel;
    /**
     * kryo serialization configuration
     */
    private DecoratedKryo kryo;
    /**
     * off-heap memory manager abstraction(SSD,OS).
     */
    private EffectiveMemoryManager memoryManager;
    /**
     * jspace executor service
     */
    private ListeningExecutorService executorService;
    /**
     * jspace scheduler executor service
     */
    private ListeningScheduledExecutorService scheduledExecutorService;
    /**
     * default network communication timeout.
     */
    private long defaultCommunicationTimeout = defaultCommunicationTimeout();

    /**
     * space deployment topology
     */
    SpaceTopology topology = SpaceTopology.SYNC_REPLICATED;

    private final ConcurrentMap<Class<?>, BO> bos = SpaceUtility.newCompMap( new Function<Class<?>, BO>() {
        @SuppressWarnings("unchecked")
        @Override
        public BO apply(final Class<?> input) {
            for ( ;; )
                try {
                    return new BO( (BasicPersistentEntity) getMappingContext().getPersistentEntity( input ) );
                }
                catch ( Exception e ) {
                    logger.error( e.getMessage(), e );
                    Throwables.propagate( e );
                }
        }
    } );

    @Override
    public void setApplicationContext(final ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    /**
     * set custom kryo serialization instance. typically you would prefer to use default one, but just in case you want
     * to do something special or add some custom serializers.</p>
     * 
     * @param kryo
     *            custom kryo configuration(and implementation)
     */
    public void setKryo(final DecoratedKryo kryo) {
        this.kryo = Preconditions.checkNotNull( kryo );
    }

    /**
     * set the custom(something different from jgroups default configuration). typically you would use default
     * configuration, but in case of advanced jgroups users... </p>
     * 
     * also another typical case for using custom channel configuration is simply registering view listeners via
     * {@link JChannel#removeChannelListener(org.jgroups.ChannelListener)} </p>
     * 
     * @param jChannel
     *            custom communication channel
     */
    public void setjChannel(final JChannel jChannel) {
        this.jChannel = Preconditions.checkNotNull( jChannel );
    }

    /**
     * each jspace has its own unique name which is exposed through network and can be used by remote client to connect
     * to the cluster(or particular node). typically you would set something meaningful because default value is just
     * <b>turbospaces-jspace-group</b>. Good example are: userSpace, contactsSpace ,flightSpace, inventorySpace
     * etc... </p>
     * 
     * @param spaceLogicalName
     *            logical(and network lookup) group
     */
    public void setGroup(final String spaceLogicalName) {
        this.group = Preconditions.checkNotNull( spaceLogicalName );
    }

    /**
     * set preferred mapping context. currently turbospaces is built on top of spring-data's abstraction, so you would
     * choose persistent provider(mongoDB, JPA), then pick appropriate implementation and inject mappingContext here.
     * </p>
     * 
     * @param mappingContext
     *            Concrete mapping context implementation.
     */
    public void setMappingContext(final AbstractMappingContext mappingContext) {
        this.mappingContext = Preconditions.checkNotNull( mappingContext );
    }

    /**
     * @return the default network communication timeout
     */
    public long getDefaultCommunicationTimeoutInMillis() {
        return defaultCommunicationTimeout;
    }

    /**
     * change the default communication timeout (which is about <b>5 seconds</b>) to new value in milliseconds
     * 
     * @param defaultCommunicationTimeout
     *            new timeout in milliseconds
     */
    public void setDefaultCommunicationTimeoutInMillis(final long defaultCommunicationTimeout) {
        this.defaultCommunicationTimeout = defaultCommunicationTimeout;
    }

    /**
     * associate executor service with jspace
     * 
     * @param executorService
     *            asynchronous task executor
     */
    public void setExecutorService(final ExecutorService executorService) {
        this.executorService = MoreExecutors.listeningDecorator( Preconditions.checkNotNull( executorService ) );
    }

    /**
     * associate scheduled executor service
     * 
     * @param scheduledExecutorService
     *            asynchronous task executor
     */
    public void setScheduledExecutorService(final ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = MoreExecutors.listeningDecorator( Preconditions.checkNotNull( scheduledExecutorService ) );
    }

    /**
     * associate custom off-heap memory manager with the space(it can be SSD cache for example).
     * 
     * @param memoryManager
     *            custom memory manager
     */
    public void setMemoryManager(final EffectiveMemoryManager memoryManager) {
        this.memoryManager = memoryManager;
    }

    /**
     * @return entity mapping context associated with this configuration(this is abstract spring-data's class which
     *         hides actual persistent storage's mapping details).
     */
    public AbstractMappingContext getMappingContext() {
        return mappingContext;
    }

    /**
     * @return the jspace topology associated with this space
     */
    public SpaceTopology getTopology() {
        return topology;
    }

    /**
     * @return the network communication jChannel
     */
    public JChannel getJChannel() {
        return jChannel;
    }

    /**
     * @return message dispatcher associated with the jChannel.
     */
    public ServerCommunicationDispatcher getMessageDispatcher() {
        return (ServerCommunicationDispatcher) getJChannel().getReceiver();
    }

    /**
     * get the kryo serialization instance associated with jspace.
     * 
     * @return kryo serializer
     */
    public DecoratedKryo getKryo() {
        return kryo;
    }

    /**
     * @return the jspace logical name (and group in term of networking discovery)
     */
    public String getGroup() {
        return group;
    }

    /**
     * @return executor service associated with jspace
     */
    public ListeningExecutorService getListeningExecutorService() {
        return executorService;
    }

    /**
     * @return scheduled executor service associated with jspace
     */
    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    /**
     * @return off-heap memory manager associated with jspace
     */
    public EffectiveMemoryManager getMemoryManager() {
        return memoryManager;
    }

    @Override
    public void destroy() {
        if ( jChannel != null ) {
            jChannel.disconnect();
            jChannel.close();
        }
        if ( executorService != null )
            executorService.shutdown();
    }

    /**
     * 1. initialize jchannel
     * 2. initialize conversion service
     * 3. initialize mapping context
     * 4. initialize kryo
     */
    @Override
    @SuppressWarnings("unchecked")
    public void afterPropertiesSet()
                                    throws Exception {
        logger.info( "Initializing JSpace configuration: group = {}", getGroup() );

        if ( getJChannel() == null ) {
            ClassPathResource largeClusterCfg = new ClassPathResource( "turbospaces-jgroups-udp.xml" );
            InputStream inputStream = largeClusterCfg.getInputStream();
            setjChannel( new JChannel( inputStream ) );
            inputStream.close();
        }
        getJChannel().setDiscardOwnMessages( true );

        if ( getMemoryManager() == null )
            setMemoryManager( new UnsafeMemoryManager() );
        if ( getMappingContext() == null )
            if ( applicationContext != null )
                setMappingContext( applicationContext.getBean( AbstractMappingContext.class ) );

        if ( getListeningExecutorService() == null )
            setExecutorService( Executors.newFixedThreadPool(
                    1 << 6,
                    new ThreadFactoryBuilder().setDaemon( false ).setNameFormat( "jspace-execpool-thread-%s" ).build() ) );
        if ( getScheduledExecutorService() == null )
            setScheduledExecutorService( Executors.newScheduledThreadPool(
                    1 << 2,
                    new ThreadFactoryBuilder().setDaemon( true ).setNameFormat( "jspace-scheduledpool-thread-%s" ).build() ) );

        Preconditions.checkState( mappingContext != null, MAPPING_CONTEXT_IS_NOT_REGISTERED );

        Collection<BasicPersistentEntity> persistentEntities = mappingContext.getPersistentEntities();
        for ( BasicPersistentEntity e : persistentEntities )
            boFor( e.getType() );

        if ( kryo == null )
            kryo = new DecoratedKryo();
        SpaceUtility.registerSpaceClasses( this, kryo );
    }

    /**
     * get the {@link BO} class wrapper for target class (retrieve from internal cache)
     * 
     * @param clazz
     *            space entity class
     * @return cached BO class wrapper
     */
    public BO boFor(final Class<?> clazz) {
        return bos.get( clazz );
    }

    /**
     * restrict space capacity on class level for particular class
     * 
     * @param clazz
     *            class that needs capacity restriction
     * @param restriction
     *            capacity restrictor
     * @return BO for given class
     */
    public BO restrictCapacity(final Class<?> clazz,
                               final CapacityRestriction restriction) {
        Preconditions.checkNotNull( restriction );
        BO bo = boFor( clazz );
        bo.setCapacityRestriction( restriction );
        return bo;
    }

    protected void dumpConfiguration() {
        logger.info( "Kryo: {}", kryo );
        logger.info( "MappingContext: {}", mappingContext );
        logger.info( "JChannel: \n {}", jChannel.toString( true ) );

        logger.info( "JSpace configuration initialization finished: group = {}", getGroup() );
    }

    /**
     * join the network and begin communications with other nodes
     * 
     * @throws Exception
     *             re-throw jgroup's exception
     */
    public void joinNetwork()
                             throws Exception {
        logger.info( "{} joining network group {}", getJChannel().getName(), group );
        jChannel.connect( group );
    }

    /**
     * @return default jspace group name if no specified by client (typical case for such scenario is product
     *         evaluation).
     */
    public static String defaultGroupName() {
        return String.format( "jspace-%s-v-%s", System.getProperty( "user.name" ), SpaceUtility.projectVersion() );
    }

    /**
     * @return default network lookup/communication timeout in milliseconds
     * @see #setDefaultCommunicationTimeoutInMillis(long)
     */
    public static long defaultCommunicationTimeout() {
        return DEFAULT_COMMUNICATION_TIMEOUT;
    }

    /**
     * @return default period for scheduled automatic cache cleanup activities in milliseconds
     */
    public static long defaultCacheCleanupPeriod() {
        return DEFAULT_CACHE_CLEANUP_PERIOD;
    }

    /**
     * Return the default timeout that this transaction manager should apply
     * if there is no timeout specified at the transaction level, in seconds.
     * 
     * @return value in seconds
     * @see DefaultTransactionDefinition#setTimeout(int)
     * @see SpaceTransactionManager#setDefaultTimeout(int)
     */
    public static int defaultTransactionTimeout() {
        return DEFAULT_TRANSACTION_TIMEOUT;
    }
}
