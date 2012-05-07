package com.turbospaces.network;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.springframework.remoting.RemoteAccessException;

import com.turbospaces.network.MethodCall.BeginTransactionMethodCall;

@SuppressWarnings("javadoc")
public class NetworkCommunicationDispatcherTest {
    Address address1 = UUID.randomUUID();
    Address address2 = UUID.randomUUID();
    Address address3 = UUID.randomUUID();
    Message[] messages;

    @Before
    public void setup() {
        messages = new Message[3];

        Message message1 = new Message();
        Message message2 = new Message();
        Message message3 = new Message();
        message1.setDest( address1 );
        message2.setDest( address2 );
        message3.setDest( address3 );
        messages[0] = message1;
        messages[1] = message2;
        messages[2] = message3;
    }

    @Test
    public void testSunnyDayScenario() {
        MethodCall request = new BeginTransactionMethodCall();
        NetworkCommunicationDispatcher.verifyNoExceptions( dummyResponses(), messages, request );
    }

    @Test(expected = RemoteAccessException.class)
    public void testWithFailures() {
        MethodCall request = new BeginTransactionMethodCall();
        MethodCall[] dummyResponses = dummyResponses();
        IllegalArgumentException argumentException1 = new IllegalArgumentException( String.valueOf( System.currentTimeMillis() ) );
        argumentException1.fillInStackTrace();
        dummyResponses[1].setException( argumentException1 );
        IllegalArgumentException argumentException2 = new IllegalArgumentException( String.valueOf( System.currentTimeMillis() ) );
        argumentException2.fillInStackTrace();
        dummyResponses[2].setException( argumentException2 );
        NetworkCommunicationDispatcher.verifyNoExceptions( dummyResponses, messages, request );
    }

    private static MethodCall[] dummyResponses() {
        MethodCall[] methodCalls = new MethodCall[3];
        methodCalls[0] = new MethodCall();
        methodCalls[1] = new MethodCall();
        methodCalls[2] = new MethodCall();
        return methodCalls;
    }
}
