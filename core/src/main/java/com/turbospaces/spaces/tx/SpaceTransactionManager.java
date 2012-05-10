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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.ResourceTransactionManager;

import com.google.common.base.Preconditions;
import com.turbospaces.api.AbstractSpaceConfiguration;
import com.turbospaces.api.ClientSpaceConfiguration;
import com.turbospaces.api.JSpace;
import com.turbospaces.spaces.RemoteJSpace;
import com.turbospaces.spaces.TransactionalJSpace;

/**
 * Space transaction manager allow you to communicate with {@link JSpace} transactionally.
 * Currently nested transactions are not allowed and not supported - however this can be changed in future releases.
 * 
 * @since 0.1
 */
@SuppressWarnings("serial")
public class SpaceTransactionManager extends AbstractPlatformTransactionManager implements ResourceTransactionManager, InitializingBean {
    private transient final Logger log = LoggerFactory.getLogger( getClass() );

    /**
     * jspace itself
     */
    private transient TransactionalJSpace jSpace;
    /**
     * is this proxy transaction manager(remote transaction manager) - meaning that the actual transaction modifications
     * will be stored on the server machine, not client one.
     * 
     * @see RemoteJSpace
     */
    private boolean proxyMode;

    @Override
    protected Object doGetTransaction() {
        SpaceTransactionObject txObject = new SpaceTransactionObject();
        SpaceTransactionHolder transactionHolder = getResourceFactory().getTransactionHolder();
        txObject.setSpaceTransactionHolder( transactionHolder );
        return txObject;
    }

    @Override
    protected void doBegin(final Object transaction,
                           final TransactionDefinition definition) {
        SpaceTransactionObject txObject = (SpaceTransactionObject) transaction;
        Object mc = proxyMode ? TransactionModificationContextProxy.borrowObject() : TransactionModificationContext.borrowObject();
        SpaceTransactionHolder transactionHolder = new SpaceTransactionHolder();
        transactionHolder.setModificationContext( mc );
        txObject.setSpaceTransactionHolder( transactionHolder );
        txObject.getSpaceTransactionHolder().setSynchronizedWithTransaction( true );
        txObject.getSpaceTransactionHolder().setTimeoutInSeconds( determineTimeout( definition ) );
        getResourceFactory().bindTransactionHolder( txObject.getSpaceTransactionHolder() );
    }

    @Override
    protected boolean isExistingTransaction(final Object transaction) {
        SpaceTransactionObject txObject = (SpaceTransactionObject) transaction;
        return txObject.getSpaceTransactionHolder() != null;
    }

    @Override
    protected void doCommit(final DefaultTransactionStatus status) {
        sync( status, true );
    }

    @Override
    protected void doRollback(final DefaultTransactionStatus status) {
        sync( status, false );
    }

    @Override
    protected void doCleanupAfterCompletion(final Object transaction) {
        SpaceTransactionObject txObject = (SpaceTransactionObject) transaction;
        getResourceFactory().unbindTransactionHolder( txObject.getSpaceTransactionHolder() );
    }

    @Override
    protected Object doSuspend(final Object transaction) {
        SpaceTransactionObject txObject = (SpaceTransactionObject) transaction;
        txObject.setSpaceTransactionHolder( null );
        return getResourceFactory().unbindTransactionHolder( txObject.getSpaceTransactionHolder() );
    }

    @Override
    protected void doResume(final Object transaction,
                            final Object suspendedResources) {
        SpaceTransactionHolder transactionHolder = (SpaceTransactionHolder) suspendedResources;
        getResourceFactory().bindTransactionHolder( transactionHolder );
    }

    @Override
    protected void doSetRollbackOnly(final DefaultTransactionStatus status) {
        SpaceTransactionObject txObject = (SpaceTransactionObject) status.getTransaction();
        txObject.setRollbackOnly();
    }

    @Override
    public TransactionalJSpace getResourceFactory() {
        return jSpace;
    }

    /**
     * set {@link JSpace} and associate this transaction manager with given java space. this property is required.
     * 
     * @param jSpace
     */
    public void setjSpace(final TransactionalJSpace jSpace) {
        this.jSpace = jSpace;
        this.proxyMode = jSpace.getSpaceConfiguration() instanceof ClientSpaceConfiguration;
    }

    @Override
    public void afterPropertiesSet() {
        setNestedTransactionAllowed( false );
        Preconditions.checkNotNull( jSpace, "SpaceTransactionManager can't be initialized: 'jSpace' is missing" );

        {
            // set default transaction timeout implicitly in seconds
            if ( getDefaultTimeout() == TransactionDefinition.TIMEOUT_DEFAULT ) {
                log.info( "setting default transaction timeout implicitly to {} seconds", AbstractSpaceConfiguration.defaultTransactionTimeout() );
                setDefaultTimeout( AbstractSpaceConfiguration.defaultTransactionTimeout() );
            }
        }
    }

    /**
     * synchronize transaction (either commit or rollback)
     * 
     * @param status
     * @param commit
     */
    private void sync(final DefaultTransactionStatus status,
                      final boolean commit) {
        Object modificationContext = null;
        try {
            SpaceTransactionObject txObject = (SpaceTransactionObject) status.getTransaction();
            txObject.flush();
            SpaceTransactionHolder transactionHolder = txObject.getSpaceTransactionHolder();
            modificationContext = transactionHolder.getModificationContext();
            if ( !proxyMode ) {
                TransactionModificationContext c = (TransactionModificationContext) modificationContext;
                if ( c.isDirty() )
                    getResourceFactory().syncTx( c, commit );
            }
            else {
                TransactionModificationContextProxy c = (TransactionModificationContextProxy) modificationContext;
                if ( c.isDirty() )
                    getResourceFactory().syncTx( c, commit );
            }
        }
        finally {
            if ( proxyMode )
                TransactionModificationContextProxy.recycle( (TransactionModificationContextProxy) modificationContext );
            else
                TransactionModificationContext.recycle( (TransactionModificationContext) modificationContext );
        }
    }
}
