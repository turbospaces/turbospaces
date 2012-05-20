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
package com.turbospaces.spaces.tx;

import java.util.HashMap;
import java.util.Map;

import org.jgroups.Address;

import com.turbospaces.pool.ObjectFactory;
import com.turbospaces.pool.ObjectPool;
import com.turbospaces.pool.SimpleObjectPool;

/**
 * remote transaction modification context proxy - the actual modification context is held on server and the role of
 * this class is just mirror between server and client.
 * 
 * @since 0.1
 */
public final class TransactionModificationContextProxy {
    private static final ObjectPool<TransactionModificationContextProxy> OBJECT_POOL;

    static {
        OBJECT_POOL = new SimpleObjectPool<TransactionModificationContextProxy>( new ObjectFactory<TransactionModificationContextProxy>() {
            @Override
            public TransactionModificationContextProxy newInstance() {
                return new TransactionModificationContextProxy();
            }

            @Override
            public void invalidate(final TransactionModificationContextProxy tx) {
                tx.transactionIds.clear();
            }
        } );
    }

    /**
     * the list of remote transaction id-s for each server(for both partitioned and replicated jspace).
     */
    private final Map<Address, Long> transactionIds = new HashMap<Address, Long>();

    private TransactionModificationContextProxy() {}

    /**
     * @return pool instance
     */
    public static TransactionModificationContextProxy borrowObject() {
        return OBJECT_POOL.borrowObject();
    }

    /**
     * return object to pool
     * 
     * @param context
     *            modication context
     */
    public static void recycle(final TransactionModificationContextProxy context) {
        OBJECT_POOL.returnObject( context );
    }

    /**
     * assign transaction id for the remote address
     * 
     * @param address
     *            off-heap memory address
     * @param transactionId
     *            transaction identifier
     * @return given address itself
     */
    public long assignTransactionId(final Address address,
                                    final Long transactionId) {
        transactionIds.put( address, transactionId );
        return transactionId;
    }

    /**
     * get the assigned remote transaction id
     * 
     * @param address
     *            off-heap memoryt address
     * @return transaction id
     */
    public Long getAssignedTransactionId(final Address address) {
        return transactionIds.get( address );
    }

    /**
     * check if transaction id has been assigned for the given address previously
     * 
     * @param address
     *            off-heap memory address
     * @return true if transaction id has been assigned previously
     */
    public boolean hasAssignedId(final Address address) {
        return transactionIds.get( address ) != null;
    }

    /**
     * @return true if transaction modification context is dirty (has remote transactions created)
     */
    public boolean isDirty() {
        return !transactionIds.isEmpty();
    }

    /**
     * @return transaction id's
     */
    public Map<Address, Long> getTransactionIds() {
        return transactionIds;
    }
}
