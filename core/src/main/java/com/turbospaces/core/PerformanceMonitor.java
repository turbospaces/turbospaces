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

import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.jgroups.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.turbospaces.api.JSpace;
import com.turbospaces.pool.ObjectFactory;
import com.turbospaces.pool.SimpleObjectPool;
import com.turbospaces.spaces.AbstractJSpace;
import com.turbospaces.spaces.SimplisticJSpace;

/**
 * utility class for performance results measurement. allows to set overall number of iteration, number of concurrent
 * threads, write/take/read percentage and provided reasonable default values.
 * 
 * @since 0.1
 */
public class PerformanceMonitor implements Runnable {
    private final Logger logger = LoggerFactory.getLogger( getClass() );
    private int threadsCount;
    private int numberOfIterations;
    private int putPercentage, getPercentage;

    private final SimplisticJSpace space;
    private final FastObjectFactory objectFactory;
    private boolean matchById;

    /**
     * create performance monitor with space and object factory(factory responsible for instantiating test entities).
     * 
     * @param space
     *            java space or some kind of proxy
     * @param objectFactory
     */
    public PerformanceMonitor(final JSpace space, final FastObjectFactory objectFactory) {
        this.space = space instanceof SimplisticJSpace ? (SimplisticJSpace) space : new SimplisticJSpace( (AbstractJSpace) space );
        this.objectFactory = objectFactory;
    }

    @SuppressWarnings({ "javadoc" })
    public PerformanceMonitor applyDefaultSettings() {
        withThreadsCount( Runtime.getRuntime().availableProcessors() * 2 );
        withNumberOfIterations( 1000 * 1000 * 10 );
        withPutPercentage( 20 );
        withGetPercentage( 60 );
        withMatchById( true );

        return this;
    }

    @SuppressWarnings("javadoc")
    public PerformanceMonitor withThreadsCount(final int i) {
        threadsCount = i;
        return this;
    }

    @SuppressWarnings("javadoc")
    public PerformanceMonitor withNumberOfIterations(final int i) {
        numberOfIterations = i;
        return this;
    }

    @SuppressWarnings("javadoc")
    public PerformanceMonitor withGetPercentage(final int i) {
        getPercentage = i;
        return this;
    }

    @SuppressWarnings("javadoc")
    public PerformanceMonitor withPutPercentage(final int i) {
        putPercentage = i;
        return this;
    }

    @SuppressWarnings("javadoc")
    public PerformanceMonitor withMatchById(final boolean b) {
        matchById = b;
        return this;
    }

    @Override
    public void run() {
        Preconditions.checkNotNull( objectFactory );
        Preconditions.checkNotNull( space );

        Preconditions.checkArgument(
                getPercentage + putPercentage <= 100,
                String.format( "getPercantage %s + putPercentage %s > 100", getPercentage, putPercentage ) );
        Preconditions.checkArgument( numberOfIterations > 0, "numberOfIteration can't be negative" );
        Preconditions.checkArgument( threadsCount > 0, "threadsCount can't be negative" );

        logger.info( " Starting pefrormance test run... " );
        logger.info( " Threads Count     : {}", threadsCount );
        logger.info( " Iterations Count  : {}", numberOfIterations );
        logger.info( " Get  Percentage   : {}", getPercentage );
        logger.info( " Put  Percentage   : {}", putPercentage );
        logger.info( " Take Percentage   : {}", putPercentage );

        final CacheStatistics cacheStatistics = new CacheStatistics();
        final AtomicBoolean completitionSemapshore = new AtomicBoolean( false );

        Executors.newSingleThreadExecutor( new ThreadFactory() {
            @Override
            public Thread newThread(final Runnable r) {
                Thread t = new Thread( r );
                t.setName( "PerformanceMonitor-Thread" );
                return t;
            }
        } ).execute( new Runnable() {

            @Override
            public void run() {
                while ( !completitionSemapshore.get() )
                    try {
                        Thread.sleep( 1000 );
                        CacheStatistics statistics = cacheStatistics.clone();
                        cacheStatistics.reset();
                        long total = statistics.getHitsCount() + statistics.getPutsCount() + statistics.getTakesCount();

                        if ( total == 0 && !completitionSemapshore.get() ) {
                            System.err.println( space.toString() );
                            System.err.println( Util.dumpThreads() );
                        }
                        else
                            logger.info( "total TPS = {} :=> {}", total, statistics.toString() );
                    }
                    catch ( InterruptedException e ) {
                        e.printStackTrace();
                        Thread.currentThread().interrupt();
                    }
            }
        } );

        try {
            LinkedList<Throwable> exceptions = SpaceUtility.repeatConcurrently( threadsCount, numberOfIterations, new Function<Integer, Object>() {
                SimpleObjectPool<Object> pool = new SimpleObjectPool<Object>( objectFactory );
                Object obj = objectFactory.newInstance();
                Class<?> clazz = obj.getClass();
                Random random = new Random();

                @Override
                public Object apply(final Integer iteration) {
                    int key = random.nextInt( numberOfIterations );
                    int action = random.nextInt( 100 );

                    if ( action < getPercentage ) {
                        if ( matchById )
                            space.readByID( Integer.valueOf( key ).toString(), clazz );
                        cacheStatistics.increaseHitsCount();
                    }
                    else if ( action < getPercentage + putPercentage ) {
                        Object borrowObject = pool.borrowObject();
                        try {
                            space.write( objectFactory.setId( borrowObject, Integer.valueOf( iteration ).toString() ) );
                            cacheStatistics.increasePutsCount();
                        }
                        finally {
                            pool.returnObject( borrowObject );
                        }
                    }
                    else {
                        if ( matchById )
                            space.takeByID( Integer.valueOf( key ).toString(), clazz );
                        cacheStatistics.increaseTakesCount();
                    }
                    return this;
                }
            } );
            logger.info( " inishing pefrormance test run: total_entities = {}, mb = {}, space dump = {}", new Object[] { space.size(),
                    space.mbUsed(), space.toString() } );
            Assert.assertTrue( "Errors = " + exceptions, exceptions.isEmpty() );
        }
        catch ( Exception e ) {
            logger.error( e.getMessage(), e );
            Throwables.propagate( e );
        }
        finally {
            completitionSemapshore.set( true );
        }
    }

    /**
     * optimized version of object factory which allows to set id (without reflection).
     */
    public static interface FastObjectFactory extends ObjectFactory<Object> {
        /**
         * set unique identifier value on target object.
         * 
         * @param target
         * 
         * @param id
         *            id to set
         * @return this pre-populated with ID.
         */
        Object setId(Object target,
                     Object id);
    }
}
