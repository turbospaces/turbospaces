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
package com.elsecloud.api;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.remoting.RemoteConnectFailureException;
import org.springframework.remoting.RemoteInvocationFailureException;
import org.springframework.remoting.RemoteLookupFailureException;
import org.springframework.transaction.annotation.Transactional;

/**
 * core interface of the system which encapsulates concept of tuple spaces.
 * 
 * </p> Object Spaces is a paradigm for development of distributed computing
 * applications. All the participants of the distributed application share an
 * Object Space. A provider of a service encapsulates the service as an Object,
 * and puts it in the Object Space. Clients of a service then access the Object
 * Space, find out which object provides the needed service, and have the
 * request serviced by the object. Object Spaces, as a computing paradigm, was
 * put forward by David Gelernter at Yale University. Gelernter developed a
 * language called Linda to support the concept of global object coordination.
 * Object Space can be thought of as a virtual repository, shared amongst
 * providers and accessors of network services, which are themselves abstracted
 * as objects. Processes communicate among each other using these shared objects
 * — by updating the state of the objects as and when needed. An object, when
 * deposited into a space, needs to be registered with an Object Directory in
 * the Object Space. Any processes can then identify the object from the Object
 * Directory, using properties lookup, where the property specifying the
 * criteria for the lookup of the object is its name or some other property
 * which uniquely identifies it. A process may choose to wait for an object to
 * be placed in the Object Space, if the needed object is not already present.
 * Objects, when deposited in an Object Space are passive, i.e., their methods
 * cannot be invoked while the objects are in the Object Space. Instead, the
 * accessing process must retrieve it from the Object Space into its local
 * memory, use the service provided by the object, update the state of the
 * object and place it back into the Object Space. This paradigm inherently
 * provides mutual exclusion. Because once an object is accessed, it has to be
 * removed from the Object Space, and is placed back only after it has been
 * released. This means that no other process can access an object while it is
 * being used by one process, thereby ensuring mutual exclusion.</p>
 * 
 * There are 5 basic methods allowing you to interact with the Objects Space:
 * write, read, take, update, notify. Instead of re-usage of JINI JavaSpaces
 * API, this interface is being created. And of course there are number of
 * reasons:
 * <ul>
 * <li>jini itself is almost dead project and there is only one commercial successor - Gigaspaces with actual
 * implementation far away from just space concepts.</li>
 * <li>Javaspace API forces to use JINI - which is actually something we are trying to omit</li>
 * <li>JINI is not installed with JRE and requires additional application dependencies</li>
 * <li>instead of using JINI API, spring transaction and DAO exception abstraction to be reused</li>
 * </ul>
 * 
 * </p>
 * <strong>NOTE:</strong> that space interactions could be executed under transactions. Typically you would use Spring's
 * transactions abstraction and configure transaction point-cuts via AOP
 * (AspectJ or just Spring AOP) via spring configuration file or {@link Transactional} Transactional annotations.
 * 
 * @sine 0.1
 */
public interface JSpace extends InitializingBean, DisposableBean {

    /**
     * send notification to the listeners if entity was inserted into the space and matches with template. Currently
     * notification will happen after transaction completition only. Additionally you may want to specify
     * {@link #MATCH_BY_ID} modifier and be notified if matching occurs by primary key (all other fields are not taking
     * part in matching process).
     * 
     * @param template
     *            Java Object, basically just POJO
     * @param listener
     * @param modifiers
     * 
     * @throws RemoteConnectFailureException
     *             for remote jspace proxy and for communication errors between client and server this exception being
     *             raised
     * @throws RemoteLookupFailureException
     *             for remote jspace proxy in case when no remote server are being available indicates that client
     *             unable to lookup any of remote server with-in some pre-configured timeout
     * @throws RemoteInvocationFailureException
     *             for remote jspace proxy indicates that server was not able to execute method due to user/internal
     *             exception
     */
    void notify(@Nonnull Object template,
                @Nonnull SpaceNotificationListener listener,
                int modifiers)
                              throws RemoteConnectFailureException,
                              RemoteLookupFailureException,
                              RemoteInvocationFailureException;

    /**
     * read/take java objects from the space by template, waiting for the concurrent "write" (or "exclusive-read")
     * transaction to commit/rollback with the given timeout if necessary (concurrent "write" transaction means parallel
     * transaction which is trying to delete existing entity with the same primary key or trying to update existing
     * entity by the same unique identifier).</p>
     * 
     * Also note that just read is not blocked regardless of concurrent transactions except the case
     * where concurrent transaction holds {@link #EXCLUSIVE_READ_LOCK} lock for particular key.
     * 
     * <ul>
     * <li>You can treat fetch as database select by template with the small difference - read operation can be blocked
     * in case of parallel exclusive read lock transaction and you will wait until the exclusive read is unlocked or
     * timeout exceeded.</li>
     * 
     * <li>
     * You can treat fetch with {@link #TAKE_ONLY} modifier as database delete with small difference - you can specify
     * timeout of delete operation.</li>
     * 
     * 
     * <li>Also you can treat fetch with {@link #EVICT_ONLY} modifier exactly the same as with {@link #TAKE_ONLY} with
     * small difference - evict does not remove entity from external data source if such data source configured.</li>
     * </ul>
     * 
     * @param template
     *            Java Object, basically just POJO
     * @param timeout
     *            allows to wait for concurrent transaction to complete if
     *            any (in milliseconds).
     * @param maxResults
     *            in case of multiple matches in space specify the maximum numbers of records to be fetched/deleted
     * @param modifiers
     * @return collection of matched objects
     * 
     * @throws CannotAcquireLockException
     *             if concurrent "write" (or "exclusive-read") transaction
     *             trying to update/delete the same object and is not completed within given
     *             timeout.
     * @throws DataAccessException
     *             in case of using External DataSource storage (database) throws JDBC or any other exception.
     *             another problem can be that you are using write-behind component and this component is not available
     *             for some reason(should not happen, but just in case).
     * @throws RemoteConnectFailureException
     *             for remote jspace proxy and for communication errors between client and server this exception being
     *             raised
     * @throws RemoteLookupFailureException
     *             for remote jspace proxy in case when no remote server are being available indicates that client
     *             unable to lookup any of remote server with-in some pre-configured timeout
     * @throws RemoteInvocationFailureException
     *             for remote jspace proxy indicates that server was not able to execute method due to user/internal
     *             exception
     */
    Object[] fetch(@Nonnull Object template,
                   @Nonnegative long timeout,
                   @Nonnegative int maxResults,
                   int modifiers)
                                 throws CannotAcquireLockException,
                                 DataAccessException,
                                 RemoteConnectFailureException,
                                 RemoteLookupFailureException,
                                 RemoteInvocationFailureException;

    /**
     * write(insert/update) java object into the space, waiting for the concurrent "write" transaction to
     * commit/rollback within given timeout (concurrent "write" transaction means parallel
     * transaction which is trying to delete existing entity with the same primary key or trying to update existing
     * entity by the same unique identifier).</p>
     * 
     * NOTE: you can specify modifiers you would like to use with write operation: {@link #WRITE_ONLY},
     * {@link #WRITE_OR_UPDATE}, {@link #UPDATE_ONLY} to force specific behavior.</p>
     * 
     * You can imagine this as database insert/update with 2 differences:
     * expiration(timeToLive) parameter can be specified and you can wait with
     * the given timeout to let concurrent transaction rollback transaction and
     * this successfully complete.
     * 
     * @param entry
     *            Java Object, basically just POJO
     * @param timeToLive
     *            allows to control expiration in milliseconds. Entity
     *            will be automatically removed after timeToLive time.
     * @param timeout
     *            allows to wait for concurrent transaction to complete if
     *            any (in milliseconds).
     * @param modifiers
     *            write-only, update-only, or write-update semantic
     * 
     * @throws DuplicateKeyException
     *             in case unique constraint violated (primary key,
     *             any other unique index), typically in case of writing duplicate
     *             entity(with the same primary key).
     * @throws CannotAcquireLockException
     *             in case concurrent "write" transaction
     *             trying to write/delete the same object and is not completed within given
     *             timeout.
     * @throws DataRetrievalFailureException
     *             in case you are trying to update entity (passing {@link #UPDATE_ONLY} modifier) and entity can't be
     *             retrieved by primary key
     * @throws DataAccessException
     *             in case of using External DataSource storage (database) throws JDBC or any other exception.
     *             another problem can be that you are using write-behind component and this component is not available
     *             for some reason(should not happen, but just in case).
     * @throws SpaceCapacityOverflowException
     *             if max capacity restrictions violated
     * @throws SpaceMemoryOverflowException
     *             if max memory capacity restriction violated
     * @throws RemoteConnectFailureException
     *             for remote jspace proxy and for communication errors between client and server this exception being
     *             raised
     * @throws RemoteLookupFailureException
     *             for remote jspace proxy in case when no remote server are being available indicates that client
     *             unable to lookup any of remote server with-in some pre-configured timeout
     * @throws RemoteInvocationFailureException
     *             for remote jspace proxy indicates that server was not able to execute method due to user/internal
     *             exception
     */
    void write(@Nonnull Object entry,
               @Nonnegative long timeToLive,
               @Nonnegative long timeout,
               int modifiers)
                             throws DuplicateKeyException,
                             CannotAcquireLockException,
                             DataRetrievalFailureException,
                             DataAccessException,
                             SpaceMemoryOverflowException,
                             SpaceCapacityOverflowException,
                             RemoteConnectFailureException,
                             RemoteLookupFailureException,
                             RemoteInvocationFailureException;

    /**
     * Gets number of elements in the jspace. theoretically size is not restricted by {@link Integer#MAX_VALUE} and can
     * exceed the int up to {@link Long#MAX_VALUE}
     * 
     * @return the size
     * 
     * @throws RemoteConnectFailureException
     *             for remote jspace proxy and for communication errors between client and server this exception being
     *             raised
     * @throws RemoteLookupFailureException
     *             for remote jspace proxy in case when no remote server are being available indicates that client
     *             unable to lookup any of remote server with-in some pre-configured timeout
     * @throws RemoteInvocationFailureException
     *             for remote jspace proxy indicates that server was not able to execute method due to user/internal
     *             exception
     */
    long size()
               throws RemoteConnectFailureException,
               RemoteLookupFailureException,
               RemoteInvocationFailureException;

    /**
     * how many megabytes are used by space to store all entities.
     * 
     * @return exact MB used by space to store all all entities
     * @throws RemoteConnectFailureException
     *             for remote jspace proxy and for communication errors between client and server this exception being
     *             raised
     * @throws RemoteLookupFailureException
     *             for remote jspace proxy in case when no remote server are being available indicates that client
     *             unable to lookup any of remote server with-in some pre-configured timeout
     * @throws RemoteInvocationFailureException
     *             for remote jspace proxy indicates that server was not able to execute method due to user/internal
     *             exception
     */
    int mbUsed()
                throws RemoteConnectFailureException,
                RemoteLookupFailureException,
                RemoteInvocationFailureException;

    /**
     * @return the jspace configuration associated with this space
     */
    AbstractSpaceConfiguration getSpaceConfiguration();

    /**
     * @return the deployment topology associated with space implementation
     * @throws RemoteConnectFailureException
     *             for remote jspace proxy and for communication errors between client and server this exception being
     *             raised
     * @throws RemoteLookupFailureException
     *             for remote jspace proxy in case when no remote server are being available indicates that client
     *             unable to lookup any of remote server with-in some pre-configured timeout
     * @throws RemoteInvocationFailureException
     *             for remote jspace proxy indicates that server was not able to execute method due to user/internal
     *             exception
     */
    SpaceTopology getSpaceTopology()
                                    throws RemoteConnectFailureException,
                                    RemoteLookupFailureException,
                                    RemoteInvocationFailureException;

    // LEASE
    /**
     * Entity must be stored in space forever.
     * This the most typical scenario you would work with.
     * 
     * <strong>NOTE:</strong> this does not mean that entity can't be evicted from the space later, this is just the
     * matter of writing entity without expiration initially.
     */
    int LEASE_FOREVER = Integer.MAX_VALUE;

    // WRITE MODIFIERS
    /**
     * Write only modifier. You can specify this modifier during write to the space. This works as database insert
     * and if entity already exists in space, exception will be thrown.
     */
    int WRITE_ONLY = 1 << 1;
    /**
     * Update only modifier. You can specify this modifier during write to the space. You will get exception in case
     * when updated entity was not persisted into space previously.
     */
    int UPDATE_ONLY = 1 << 2;
    /**
     * Write or update modifier. You can specify this modifier during write to the space. This works as save-or-update
     * and if entity already in space, update policy will be applied, if there is such entity previously persisted in
     * space, work as insert.</p>
     * 
     * This is default modifier.
     */
    int WRITE_OR_UPDATE = 1 << 3;

    // FETCH MODIFIERS
    /**
     * Evict modifier. You can specify this modifier during fetch from space. This will evict entity from space (not
     * from
     * database or any other external data source storage), but just from memory - you can consider this method as
     * invalidation mechanism.
     */
    int EVICT_ONLY = 1 << 4;
    /**
     * Match by ID modifier. You can specify this modifier during read/take from space. You can treat this as
     * read-take-by-id (no other field except primary key participates in template matching). Make sure primary key is
     * provided, otherwise exception will be thrown.
     */
    int MATCH_BY_ID = 1 << 5;
    /**
     * Exclusive read lock modifier. You can specify this modifier during fetch from space. This will lock entity for
     * any
     * kind of parallel reads. This approach can be used with pessimistic locking pattern.
     */
    int EXCLUSIVE_READ_LOCK = 1 << 6;
    /**
     * Take modifier. You can specify this modifier during fetch from space. This will remove entity from the space and
     * from the external data source if any configured. The huge difference between this modifier and
     * {@link #EVICT_ONLY} is
     * that evict operator would not remove entity from the external data source, but take will.
     */
    int TAKE_ONLY = 1 << 7;
    /**
     * Read modifier. You can specify this modifier during fetch from space. This will read the entity by template. You
     * can expect that the read itself is non-blocking and this is true for most cases except case when entity you are
     * trying to fetch is concurrently locked by another transaction and this parallel transaction holds
     * {@link #EXCLUSIVE_READ_LOCK} lock.
     */
    int READ_ONLY = 1 << 8;
    /**
     * Fetch modifier. You can specify this modifier during fetch from space. This will cause the data to be returned as
     * serialized byte array(this is internally used for client-server communications anyway), but still you may want to
     * do this explicitly.
     */
    int RETURN_AS_BYTES = 1 << 9;

    // TERMINOLOGY
    /**
     * Jspace service container.
     */
    String SSC = "SSC";
    /**
     * JSpace service client.
     */
    String SC = "SC";
}
