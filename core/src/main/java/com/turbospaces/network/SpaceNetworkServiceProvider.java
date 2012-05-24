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
package com.turbospaces.network;

import java.io.Externalizable;
import java.util.concurrent.Callable;

import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.blocks.executor.ExecutionService;
import org.jgroups.blocks.locking.LockService;
import org.jgroups.util.Streamable;

/**
 * this is proxy for high level services implementation(building blocks in terms of jgroups) that is used on client
 * side(and server as well). concrete services are cluster-wide lock, executor service and async/sync message
 * dispatcher.
 */
public class SpaceNetworkServiceProvider implements RequestHandler {
    private final LockService lockService;
    private final ExecutionService executionService;

    /**
     * initialize client space services provider over jchannel.
     * 
     * @param jChannel
     *            low level jchannel
     * @param server
     *            is this server side or client side service provider
     */
    public SpaceNetworkServiceProvider(final JChannel jChannel, final boolean server) {
        this.executionService = new ExecutionService( jChannel );
        this.lockService = new LockService( jChannel );
    }

    @Override
    public Object handle(final Message msg)
                                           throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * <b>From jgroups documentation:</b></p>
     * In 2.12, a new distributed locking service was added, replacing DistributedLockManager. The new
     * service is implemented as a protocol and is used via org.jgroups.blocks.locking.LockService.
     * LockService talks to the locking protocol via events. The main abstraction of a distributed lock is
     * an implementation of java.util.concurrent.locks.Lock. All lock methods are supported, however,
     * conditions are not fully supported, and still need some more testing </p>
     * 
     * refer to jgroups documentation for more details.</p>
     * 
     * @return cluster wide distributed lock service associated with jspace.
     */
    public LockService getLockService() {
        return lockService;
    }

    /**
     * <b>From jgroups documentation:</b></p>
     * In 2.12, a distributed execution service was added. The new service is implemented as a protocol
     * and is used via org.jgroups.blocks.executor.ExecutionService.
     * ExecutionService extends java.util.concurrent.ExecutorService and distributes tasks submitted
     * to it across the cluster, trying to distribute the tasks to the cluster members as evenly as possible.
     * When a cluster member leaves or dies, the tasks is was processing are re-distributed to other
     * members in the cluster.
     * ExecutionService talks to the executing protocol via events. The main abstraction is
     * an implementation of java.util.concurrent.ExecutorService. All methods are supported. The
     * restrictions are however that the {@link Callable} or Runnable must be Serializable, {@link Externalizable} or
     * {@link Streamable}. Also the result produced from the future needs to be Serializable, {@link Externalizable} or
     * {@link Streamable}. If the {@link Callable} or Runnable are not, then an IllegalArgumentException is immediately
     * thrown. If a result is not, then a NotSerializableException with the name of the class will be returned
     * to the Future as an exception cause. </p>
     * 
     * @return cluster wide executor service associated with jspace.
     */
    public ExecutionService getExecutionService() {
        return executionService;
    }
}
