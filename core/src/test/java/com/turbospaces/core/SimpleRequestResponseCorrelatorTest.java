package com.turbospaces.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.util.concurrent.atomic.AtomicReference;

import junit.framework.Assert;

import org.junit.Test;

import com.turbospaces.api.SpaceException;
import com.turbospaces.core.SimpleRequestResponseCorrelator;

@SuppressWarnings("javadoc")
public class SimpleRequestResponseCorrelatorTest {
    final SimpleRequestResponseCorrelator<Long, Long> correlator = new SimpleRequestResponseCorrelator<Long, Long>();

    @Test
    public void canGetResponseSynchInOnThread() {
        Object monitor = correlator.put( new Long( 1 ), null );
        correlator.put( new Long( 1 ), 101L );
        Long responseFor = correlator.responseFor( new Long( 1 ), monitor, 1 );

        assertThat( responseFor, is( 101L ) );
    }

    @Test
    public void canGetResponseAsynchInOnThread() {
        Object monitor = correlator.put( new Long( 2 ), null );
        new Thread( new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep( 10 );
                }
                catch ( InterruptedException e ) {
                    throw new RuntimeException( e );
                }
                correlator.put( new Long( 2 ), 102L );
            }
        } ).start();

        Long responseFor = correlator.responseFor( new Long( 2 ), monitor, 10000 );
        assertThat( responseFor, is( 102L ) );
        Assert.assertFalse( correlator.rawResponseMap().containsKey( 2L ) );
    }

    @Test
    public void canAutoRemoveMissedResponses() {
        Object monitor = correlator.put( new Long( 3 ), null );
        Long responseFor = correlator.responseFor( new Long( 3 ), monitor, 1 );
        assertThat( responseFor, is( nullValue() ) );
        Object monitor2 = correlator.put( new Long( 3 ), null );
        Assert.assertTrue( monitor == monitor2 );
        Assert.assertFalse( correlator.rawResponseMap().containsKey( 2L ) );
    }

    @Test(expected = SpaceException.class)
    public void canGetSpaceExceptionForThreadInterruption()
                                                           throws Exception {
        correlator.put( new Long( 4 ), null );
        final AtomicReference<Exception> ex = new AtomicReference<Exception>();
        Thread t = new Thread( new Runnable() {
            @Override
            public void run() {
                try {
                    correlator.responseFor( 4L, new Object(), 100000 );
                }
                catch ( Exception e ) {
                    ex.set( e );
                }
            }
        } );
        t.start();
        t.interrupt();
        t.join();
        throw ex.get();
    }
}
