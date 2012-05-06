package com.elsecloud.network;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;

import javax.annotation.concurrent.ThreadSafe;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.Receiver;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.remoting.RemoteLookupFailureException;

import com.elsecloud.api.JSpace;
import com.elsecloud.api.SpaceErrors;
import com.elsecloud.api.SpaceException;
import com.elsecloud.api.SpaceTopology;
import com.elsecloud.core.SpaceUtility;
import com.elsecloud.pool.ObjectPool;
import com.esotericsoftware.kryo.ObjectBuffer;

/**
 * central point for network communication over jgroups on the server side(minimalistic version instead of jgroups's
 * high-level building blocks for better performance and lower GC garbage allocations).
 * 
 * @since 0.1
 */
@ThreadSafe
public class ServerCommunicationDispatcher extends ReceiverAdapter implements SpaceErrors {
    private final Logger logger = LoggerFactory.getLogger( getClass() );
    private final Random random = new Random();
    private final Object monitor = new Object();

    protected final long communicationTimeout;
    protected final JChannel jChannel;
    protected final ObjectPool<ObjectBuffer> objectBufferPool = SpaceUtility.newObjectBufferPool();
    protected volatile Address[] serverNodes = new Address[0];
    protected volatile Address[] clientNodes = new Address[0];
    protected Collection<Receiver> delegates = new LinkedHashSet<Receiver>();

    /**
     * create receiver over jchannel with given default network communication timeout.
     * 
     * @param jChannel
     * @param communicationTimeout
     */
    public ServerCommunicationDispatcher(final JChannel jChannel, final long communicationTimeout) {
        this.jChannel = jChannel;
        this.communicationTimeout = communicationTimeout;
    }

    /**
     * register message listener delegate
     * 
     * @param delegate
     */
    public void addMessageReceiver(final Receiver delegate) {
        delegates.add( delegate );
    }

    /**
     * un-register message listener delegate
     * 
     * @param delegate
     */
    public void removeMessageReceiver(final Receiver delegate) {
        delegates.remove( delegate );
    }

    @Override
    public final void viewAccepted(final View view) {
        List<Address> members = view.getMembers();
        Address ownAddress = jChannel.getAddress();
        List<Address> newServersMembers = new ArrayList<Address>();
        List<Address> newClientMembers = new ArrayList<Address>();

        for ( Address address : members )
            if ( !ownAddress.equals( address ) ) {
                String logicalName = address.toString();
                if ( logicalName.startsWith( JSpace.SSC ) ) {
                    logger.info( "discovered jspace server member:" + address );
                    newServersMembers.add( address );
                }
                else if ( logicalName.startsWith( JSpace.SC ) ) {
                    logger.info( "discovered jspace client member:" + address );
                    newClientMembers.add( address );
                }
            }

        serverNodes = newServersMembers.toArray( new Address[newServersMembers.size()] );
        clientNodes = newClientMembers.toArray( new Address[newClientMembers.size()] );
        if ( serverNodes.length != 0 )
            synchronized ( monitor ) {
                monitor.notifyAll();
            }
        logger.info( view.toString() );

        for ( Receiver r : delegates )
            r.viewAccepted( view );
    }

    /**
     * get the remove server nodes(block if 0 nodes discovered)
     * 
     * @return the remote server side jspace nodes
     * @throws RemoteLookupFailureException
     *             if no remote server nodes are discovered
     */
    public final Address[] getServerNodes()
                                           throws RemoteLookupFailureException {
        final Address[] nodes = serverNodes;
        if ( nodes.length == 0 )
            synchronized ( monitor ) {
                try {
                    monitor.wait( communicationTimeout );
                }
                catch ( InterruptedException e ) {
                    logger.error( e.getMessage(), e );
                    Thread.currentThread().interrupt();
                    throw new SpaceException( e.getMessage(), e );
                }
            }
        if ( nodes.length == 0 )
            throw new RemoteLookupFailureException( String.format( LOOKUP_TIMEOUT, communicationTimeout ) );
        return nodes;
    }

    /**
     * get server nodes(all) for partitioned topology or any one for synch replicated jspace.
     * 
     * @param spaceTopology
     * @return appropriate server nodes(or node) for space topology
     */
    public final Address[] getServerNodes(final SpaceTopology spaceTopology) {
        Address[] addresses = getServerNodes();
        if ( spaceTopology.isSynchReplicated() )
            return new Address[] { addresses[random.nextInt( addresses.length )] };
        return addresses;
    }

    /**
     * @return server nodes as is (even if there are no connected server nodes)
     */
    public final Address[] getRawServerNodes() {
        return serverNodes;
    }

    /**
     * @return client nodes as is (even if there are no connected server nodes)
     */
    public final Address[] getRawClientNodes() {
        return clientNodes;
    }

    @Override
    public void receive(final Message msg) {
        for ( MessageListener ml : delegates )
            ml.receive( msg );
    }

    @Override
    public void suspect(final Address mbr) {
        for ( Receiver r : delegates )
            r.suspect( mbr );
    }
}
