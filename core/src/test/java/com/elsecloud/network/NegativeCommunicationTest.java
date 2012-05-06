package com.elsecloud.network;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.Assert;

import org.junit.Test;
import org.springframework.remoting.RemoteLookupFailureException;

import com.elsecloud.api.ClientSpaceConfiguration;
import com.elsecloud.api.SpaceException;
import com.elsecloud.model.TestEntity1;

@SuppressWarnings("javadoc")
public class NegativeCommunicationTest {

    @Test
    public void cantGetExceptionLookupFailureException()
                                                        throws Exception {
        ClientSpaceConfiguration clientConfigurationFor = TestEntity1.clientConfigurationFor();
        clientConfigurationFor.setDefaultCommunicationTimeoutInMillis( 10 );
        clientConfigurationFor.joinNetwork();
        NetworkCommunicationDispatcher receiever = clientConfigurationFor.getReceiever();
        try {
            receiever.getServerNodes();
            Assert.fail();
        }
        catch ( RemoteLookupFailureException e ) {}
        finally {
            assertThat( clientConfigurationFor.getReceiever(), is( notNullValue() ) );
            assertThat( clientConfigurationFor.getJChannel(), is( notNullValue() ) );
            clientConfigurationFor.destroy();
        }
    }

    @Test(expected = SpaceException.class)
    public void canHandleInteppurptionException()
                                                 throws Throwable {
        final AtomicReference<Throwable> er = new AtomicReference<Throwable>();
        final ClientSpaceConfiguration clientConfigurationFor = TestEntity1.clientConfigurationFor();
        clientConfigurationFor.setDefaultCommunicationTimeoutInMillis( TimeUnit.SECONDS.toMillis( 10 ) );
        clientConfigurationFor.joinNetwork();
        Thread t = new Thread( new Runnable() {

            @Override
            public void run() {
                NetworkCommunicationDispatcher receiever = clientConfigurationFor.getReceiever();
                receiever.getServerNodes();
            }
        } );
        t.setUncaughtExceptionHandler( new UncaughtExceptionHandler() {

            @Override
            public void uncaughtException(final Thread t,
                                          final Throwable e) {
                er.set( e );
            }
        } );
        t.start();
        t.interrupt();
        t.join();

        clientConfigurationFor.destroy();
        throw er.get();
    }
}
