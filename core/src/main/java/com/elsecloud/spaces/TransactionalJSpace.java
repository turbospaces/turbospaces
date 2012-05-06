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
package com.elsecloud.spaces;

import com.elsecloud.api.JSpace;
import com.elsecloud.spaces.tx.SpaceTransactionHolder;
import com.elsecloud.spaces.tx.TransactionModificationContext;
import com.elsecloud.spaces.tx.TransactionModificationContextProxy;

/**
 * extends {@link JSpace} and adds the concept of transaction management (synchronization)
 * 
 * @since 0.1
 */
public interface TransactionalJSpace extends JSpace {

    /**
     * synchronize modifications made within transaction modification context over internal space stores.
     * 
     * @param ctx
     *            transaction modification type(for local-embedded jspace it is {@link TransactionModificationContext},
     *            for
     *            remote proxy clients it is {@link TransactionModificationContextProxy})
     * @param commit
     *            commit or rollback transaction
     */
    void syncTx(Object ctx,
                boolean commit);

    /**
     * this is a convenient way of getting the space transaction bound to the thread.
     * 
     * @return transaction holder associated with the thread if any
     */
    SpaceTransactionHolder getTransactionHolder();

    /**
     * bind the space transaction wrapper to the thread
     * 
     * @param transactionHolder
     *            space transaction wrapper
     */
    void bindTransactionHolder(SpaceTransactionHolder transactionHolder);

    /**
     * un-bind the transaction from the current thread
     * 
     * @param spaceTransactionHolder
     * 
     * @return the previously bound value (usually the active resource object)
     */
    Object unbindTransactionHolder(SpaceTransactionHolder spaceTransactionHolder);
}
