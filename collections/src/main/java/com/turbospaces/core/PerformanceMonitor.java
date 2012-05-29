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

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.esotericsoftware.minlog.Log;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Uninterruptibles;
import com.lmax.disruptor.Sequence;

/**
 * utility class for performance results measurement. allows to set overall number of iteration, number of concurrent
 * threads, write/take/read percentage and provided reasonable default values.
 * 
 * @since 0.1
 */
@SuppressWarnings({ "javadoc" })
public final class PerformanceMonitor<V> implements Runnable {

    private int threadsCount;
    private int numberOfIterations;
    private int putPercentage, getPercentage;
    private final Function<Map.Entry<String, V>, V> putFunction;
    private final Function<String, V> getFunction, removeFunction;
    private final ObjectFactory<V> objectFactory;

    /**
     * create new performance monitor(runner) with user supplied put/get/remove callback functions.
     * 
     * @param putFunction
     *            function used for adding entities to cache
     * @param getFunction
     *            function used for reading entities in cache
     * @param removeFunction
     *            function used for removing entities from cache
     * @param objectFactory
     *            values instantiation factory (we need to re-use object to minimize GC impact)
     * 
     * @param returnObjects2Pool
     *            return objects to the pool after write
     */
    public PerformanceMonitor(final Function<Map.Entry<String, V>, V> putFunction,
                              final Function<String, V> getFunction,
                              final Function<String, V> removeFunction,
                              final ObjectFactory<V> objectFactory) {
        super();
        this.objectFactory = objectFactory;
        this.putFunction = Preconditions.checkNotNull( putFunction );
        this.getFunction = Preconditions.checkNotNull( getFunction );
        this.removeFunction = Preconditions.checkNotNull( removeFunction );

        applyDefaultSettings( this );
    }

    public PerformanceMonitor<V> applyDefaultSettings(final PerformanceMonitor<V> m) {
        m.withThreadsCount( Runtime.getRuntime().availableProcessors() );
        m.withNumberOfIterations( 1000 * 1000 * 10 );
        m.withPutPercentage( 20 );
        m.withGetPercentage( 60 );
        return this;
    }

    public PerformanceMonitor<V> withThreadsCount(final int i) {
        threadsCount = i;
        return this;
    }

    public PerformanceMonitor<V> withNumberOfIterations(final int i) {
        numberOfIterations = i;
        return this;
    }

    public PerformanceMonitor<V> withGetPercentage(final int i) {
        getPercentage = i;
        return this;
    }

    public PerformanceMonitor<V> withPutPercentage(final int i) {
        putPercentage = i;
        return this;
    }

    @Override
    public void run() {

        Preconditions.checkArgument(
                getPercentage + putPercentage <= 100,
                String.format( "getPercantage %s + putPercentage %s > 100", getPercentage, putPercentage ) );
        Preconditions.checkArgument( numberOfIterations > 0, "numberOfIteration can't be negative" );
        Preconditions.checkArgument( threadsCount > 0, "threadsCount can't be negative" );

        Log.info( " Starting pefrormance test run... " );
        Log.info( " Threads Count     : " + threadsCount );
        Log.info( " Iterations Count  : " + numberOfIterations );
        Log.info( " Get  Percentage   : " + getPercentage );
        Log.info( " Put  Percentage   : " + putPercentage );
        Log.info( " Take Percentage   : " + putPercentage );

        final AtomicBoolean completitionSemapshore = new AtomicBoolean( false );
        final Sequence readsHit = new Sequence( 0 );
        final Sequence readsMiss = new Sequence( 0 );
        final Sequence takesHit = new Sequence( 0 );
        final Sequence takesMiss = new Sequence( 0 );
        final Sequence writes = new Sequence( 0 );

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
                while ( !completitionSemapshore.get() ) {
                    Uninterruptibles.sleepUninterruptibly( 1, TimeUnit.SECONDS );

                    long total = readsHit.get() + readsMiss.get() + writes.get() + takesHit.get() + takesMiss.get();
                    Log.info( String.format(
                            "TPS = %s [readsHit=%s, readsMiss=%s, writes=%s, takesHit=%s, takesMiss=%s]",
                            total,
                            readsHit.get(),
                            readsMiss.get(),
                            writes.get(),
                            takesHit.get(),
                            takesMiss.get() ) );
                    readsHit.set( 0 );
                    readsMiss.set( 0 );
                    takesHit.set( 0 );
                    takesMiss.set( 0 );
                    writes.set( 0 );
                }
            }
        } );

        try {
            List<Throwable> exceptions = JVMUtil.repeatConcurrently( threadsCount, numberOfIterations, new Function<Integer, Object>() {
                private final Random random = new Random();

                @Override
                public Object apply(final Integer iteration) {
                    final int key = random.nextInt( numberOfIterations );
                    final int action = random.nextInt( 100 );

                    if ( action < getPercentage ) {
                        V v = getFunction.apply( String.valueOf( key ) );
                        if ( v != null )
                            readsHit.incrementAndGet();
                        else
                            readsMiss.incrementAndGet();
                    }
                    else if ( action < getPercentage + putPercentage ) {
                        V entryToAdd = objectFactory.newInstance();
                        putFunction.apply( new AbstractMap.SimpleEntry<String, V>( String.valueOf( key ), entryToAdd ) );
                        writes.incrementAndGet();
                    }
                    else {
                        V v = removeFunction.apply( String.valueOf( key ) );
                        if ( v != null )
                            takesHit.incrementAndGet();
                        else
                            takesMiss.incrementAndGet();
                    }
                    return this;
                }
            } );
            Preconditions.checkState( exceptions.isEmpty(), "Errors = " + exceptions );
        }
        catch ( Exception e ) {
            Log.error( e.getMessage(), e );
            Throwables.propagate( e );
        }
        finally {
            completitionSemapshore.set( true );
        }
    }
}
