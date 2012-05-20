package com.turbospaces.spaces;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.esotericsoftware.kryo.ObjectBuffer;
import com.google.common.cache.CacheBuilder;
import com.turbospaces.api.AbstractSpaceConfiguration;
import com.turbospaces.api.EmbeddedJSpaceRunnerTest;
import com.turbospaces.api.JSpace;
import com.turbospaces.api.SpaceConfiguration;
import com.turbospaces.api.SpaceTopology;
import com.turbospaces.model.TestEntity1;
import com.turbospaces.network.MethodCall;
import com.turbospaces.network.MethodCall.BeginTransactionMethodCall;
import com.turbospaces.network.MethodCall.CommitRollbackMethodCall;
import com.turbospaces.network.MethodCall.FetchMethodCall;
import com.turbospaces.network.MethodCall.GetMbUsedMethodCall;
import com.turbospaces.network.MethodCall.GetSizeMethodCall;
import com.turbospaces.network.MethodCall.GetSpaceTopologyMethodCall;
import com.turbospaces.network.MethodCall.NotifyListenerMethodCall;
import com.turbospaces.network.MethodCall.WriteMethodCall;
import com.turbospaces.network.ServerCommunicationDispatcher;

@SuppressWarnings("javadoc")
public class SpaceReceiveAdapterTest {
    SpaceConfiguration configuration;
    OffHeapJSpace heapJSpace;
    SpaceReceiveAdapter receiveAdapter;
    Address address;
    ObjectBuffer objectBuffer;
    MethodCall response;

    @Before
    public void setUp()
                       throws Exception {
        configuration = EmbeddedJSpaceRunnerTest.configurationFor();
        JChannel mock = mock( JChannel.class );
        ServerCommunicationDispatcher communicationDispatcher = new ServerCommunicationDispatcher( configuration );
        when( mock.getReceiver() ).thenReturn( communicationDispatcher );
        doAnswer( new Answer<Void>() {

            @Override
            public Void answer(final InvocationOnMock invocation)
                                                                 throws Throwable {
                Message message = (Message) invocation.getArguments()[0];
                byte[] buffer = message.getBuffer();
                response = (MethodCall) objectBuffer.readClassAndObject( buffer );
                return null;
            }
        } ).when( mock ).send( (Message) anyObject() );
        configuration.setjChannel( mock );
        heapJSpace = new OffHeapJSpace( configuration );
        heapJSpace.afterPropertiesSet();
        receiveAdapter = new SpaceReceiveAdapter( heapJSpace ) {

            @Override
            void applyExpireAfterWriteSettings(final CacheBuilder<Object, Object> builder) {
                builder.expireAfterWrite( 300, TimeUnit.MILLISECONDS );
            }

            @Override
            Address[] getClientConnections(final View view) {
                return view.getMembers().toArray( new Address[view.getMembers().size()] );
            }
        };
        address = UUID.randomUUID();
        objectBuffer = new ObjectBuffer( configuration.getKryo() );
    }

    @After
    public void tearDown()
                          throws Exception {
        heapJSpace.destroy();
        configuration.destroy();
        receiveAdapter.destroy();
    }

    @Test
    public void sunnyDayBeginTxWriteFetchCommit() {
        TestEntity1 testEntity1 = new TestEntity1();
        testEntity1.afterPropertiesSet();

        BeginTransactionMethodCall methodCall = new BeginTransactionMethodCall();
        methodCall.setTransactionTimeout( TimeUnit.SECONDS.toMillis( 1 ) );

        Message message = new Message();
        message.setSrc( address );
        message.setBuffer( objectBuffer.writeClassAndObject( methodCall ) );

        receiveAdapter.receive( message );
        Long transactionID = objectBuffer.readObjectData( response.getResponseBody(), long.class );

        NotifyListenerMethodCall notifyListenerMethodCall = new NotifyListenerMethodCall();
        notifyListenerMethodCall.setModifiers( JSpace.MATCH_BY_ID );
        notifyListenerMethodCall.setEntity( objectBuffer.writeClassAndObject( testEntity1 ) );

        message.setBuffer( objectBuffer.writeClassAndObject( notifyListenerMethodCall ) );
        receiveAdapter.receive( message );

        WriteMethodCall writeMethodCall = new WriteMethodCall();
        writeMethodCall.setTransactionId( transactionID );
        writeMethodCall.setModifiers( JSpace.WRITE_OR_UPDATE );
        writeMethodCall.setTimeout( 10034 );
        writeMethodCall.setTimeToLive( 223498 );
        writeMethodCall.setEntity( objectBuffer.writeClassAndObject( testEntity1 ) );

        message.setBuffer( objectBuffer.writeClassAndObject( writeMethodCall ) );
        receiveAdapter.receive( message );

        FetchMethodCall fetchMethodCall = new FetchMethodCall();
        fetchMethodCall.setMaxResults( 123 );
        fetchMethodCall.setModifiers( JSpace.READ_ONLY | JSpace.MATCH_BY_ID );
        fetchMethodCall.setTimeout( 23423 );
        fetchMethodCall.setTransactionId( transactionID );
        fetchMethodCall.setEntity( objectBuffer.writeClassAndObject( testEntity1 ) );

        message.setBuffer( objectBuffer.writeClassAndObject( fetchMethodCall ) );
        receiveAdapter.receive( message );

        receiveAdapter.suspect( address );

        CommitRollbackMethodCall commitRollbackMethodCall = new CommitRollbackMethodCall( true );
        commitRollbackMethodCall.setTransactionId( transactionID );

        message.setBuffer( objectBuffer.writeClassAndObject( commitRollbackMethodCall ) );
        receiveAdapter.receive( message );

        GetSizeMethodCall getSizeMethodCall = new GetSizeMethodCall();
        message.setBuffer( objectBuffer.writeClassAndObject( getSizeMethodCall ) );
        receiveAdapter.receive( message );
        Long size = objectBuffer.readObjectData( response.getResponseBody(), long.class );
        assertThat( size, is( greaterThan( 0L ) ) );

        GetMbUsedMethodCall getMbUsedMethodCall = new GetMbUsedMethodCall();
        message.setBuffer( objectBuffer.writeClassAndObject( getMbUsedMethodCall ) );
        receiveAdapter.receive( message );

        GetSpaceTopologyMethodCall getSpaceTopologyMethodCall = new GetSpaceTopologyMethodCall();
        message.setBuffer( objectBuffer.writeClassAndObject( getSpaceTopologyMethodCall ) );
        receiveAdapter.receive( message );
        SpaceTopology topology = objectBuffer.readObjectData( response.getResponseBody(), SpaceTopology.class );
        assertThat( topology, is( notNullValue() ) );
    }

    @Test
    public void canPropagateExceptionToClientCode() {
        CommitRollbackMethodCall commitRollbackMethodCall = new CommitRollbackMethodCall( false );
        commitRollbackMethodCall.setTransactionId( System.currentTimeMillis() );

        Message message = new Message();
        message.setSrc( address );
        message.setBuffer( objectBuffer.writeClassAndObject( commitRollbackMethodCall ) );

        try {
            receiveAdapter.receive( message );
        }
        catch ( RuntimeException e ) {}

        String exceptionAsString = response.getExceptionAsString();
        assertThat( exceptionAsString, is( notNullValue() ) );

        commitRollbackMethodCall = new CommitRollbackMethodCall( true );
        commitRollbackMethodCall.setTransactionId( System.currentTimeMillis() );

        message.setBuffer( objectBuffer.writeClassAndObject( commitRollbackMethodCall ) );
        try {
            receiveAdapter.receive( message );
        }
        catch ( RuntimeException e ) {}
        exceptionAsString = response.getExceptionAsString();
        assertThat( exceptionAsString, is( notNullValue() ) );
    }

    @Test
    public void canBeginRollback() {
        BeginTransactionMethodCall methodCall = new BeginTransactionMethodCall();
        methodCall.setTransactionTimeout( TimeUnit.SECONDS.toMillis( 1 ) );

        Message message = new Message();
        message.setSrc( address );
        message.setBuffer( objectBuffer.writeClassAndObject( methodCall ) );

        receiveAdapter.receive( message );
        Long transactionID = objectBuffer.readObjectData( response.getResponseBody(), long.class );

        CommitRollbackMethodCall commitRollbackMethodCall = new CommitRollbackMethodCall( false );
        commitRollbackMethodCall.setTransactionId( transactionID );
        message.setBuffer( objectBuffer.writeClassAndObject( commitRollbackMethodCall ) );
        receiveAdapter.receive( message );

        String exceptionAsString = response.getExceptionAsString();
        assertThat( exceptionAsString, is( nullValue() ) );
    }

    @Test
    public void canExpireAutomaticallyDeadTransactions()
                                                        throws InterruptedException {
        receiveAdapter.afterPropertiesSet();
        BeginTransactionMethodCall methodCall = new BeginTransactionMethodCall();
        methodCall.setTransactionTimeout( TimeUnit.SECONDS.toMillis( 1 ) );

        Message message = new Message();
        message.setSrc( address );
        message.setBuffer( objectBuffer.writeClassAndObject( methodCall ) );

        receiveAdapter.receive( message );
        Long txID = objectBuffer.readObjectData( response.getResponseBody(), long.class );

        assertThat( receiveAdapter.modificationContextFor( address ).getIfPresent( txID ), is( notNullValue() ) );
        Thread.sleep( Math.max( 360, AbstractSpaceConfiguration.defaultCacheCleanupPeriod() ) );
        assertThat( receiveAdapter.modificationContextFor( address ).getIfPresent( txID ), is( nullValue() ) );
    }

    @Test
    public void canAutomicallyRemoveUncommitedTransactions() {
        BeginTransactionMethodCall methodCall = new BeginTransactionMethodCall();
        methodCall.setTransactionTimeout( TimeUnit.SECONDS.toMillis( 1 ) );

        Message message = new Message();
        message.setSrc( address );
        message.setBuffer( objectBuffer.writeClassAndObject( methodCall ) );

        receiveAdapter.receive( message );
        Long txID = objectBuffer.readObjectData( response.getResponseBody(), long.class );

        View view1 = new View( new ViewId(), Collections.singletonList( address ) );
        receiveAdapter.viewAccepted( view1 );
        View view2 = new View( new ViewId(), Arrays.asList( new Address[] { address, UUID.randomUUID() } ) );
        receiveAdapter.viewAccepted( view2 );
        assertThat( receiveAdapter.modificationContextFor( address ).getIfPresent( txID ), is( notNullValue() ) );

        View view3 = new View( new ViewId(), new ArrayList<Address>() );
        receiveAdapter.viewAccepted( view3 );
        assertThat( receiveAdapter.modificationContextFor( address ).getIfPresent( txID ), is( nullValue() ) );
    }
}
