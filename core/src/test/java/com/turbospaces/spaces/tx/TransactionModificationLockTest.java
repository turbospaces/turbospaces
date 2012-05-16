package com.turbospaces.spaces.tx;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import com.turbospaces.core.JVMUtil;

@SuppressWarnings("javadoc")
public class TransactionModificationLockTest {

    @Test
    public void canReenterWriteLockInTheSameTransaction() {
        TransactionModificationContext c = TransactionModificationContext.borrowObject();
        TransactionModificationLock lock = new TransactionModificationLock( true );
        lock.lock( c.getTransactionId() );
        lock.lock( c.getTransactionId() );
        lock.unlock( c.getTransactionId() );

        lock.lock( c.getTransactionId() );
        lock.lock( c.getTransactionId() );
        lock.unlock( c.getTransactionId() );
    }

    @Test
    public void cantAcquireLockFromParallelTransaction()
                                                        throws InterruptedException {
        TransactionModificationContext c1 = TransactionModificationContext.borrowObject();
        TransactionModificationContext c2 = TransactionModificationContext.borrowObject();
        TransactionModificationLock lock = new TransactionModificationLock( true );

        lock.lock( c1.getTransactionId() );
        boolean b = lock.tryLock( c2.getTransactionId(), 1, TimeUnit.MILLISECONDS );
        assertFalse( b );
        b = lock.tryLock( c2.getTransactionId(), 1, TimeUnit.MILLISECONDS );
        assertFalse( b );

        lock.unlock( c1.getTransactionId() );

        b = lock.tryLock( c2.getTransactionId(), Long.MAX_VALUE, TimeUnit.MILLISECONDS );
        assertTrue( b );
    }

    @Test
    public void cantAcquireLockFromParallelThread() {
        final TransactionModificationContext c1 = TransactionModificationContext.borrowObject();
        final TransactionModificationContext c2 = TransactionModificationContext.borrowObject();
        final TransactionModificationLock lock = new TransactionModificationLock( true );
        final AtomicBoolean b = new AtomicBoolean();
        lock.lock( c1.getTransactionId() );

        new Thread() {
            @Override
            public void run() {
                try {
                    b.set( lock.tryLock( c2.getTransactionId(), Long.MAX_VALUE, TimeUnit.MILLISECONDS ) );
                }
                catch ( InterruptedException e ) {
                    e.printStackTrace();
                }
            }
        }.start();
        assertFalse( b.get() );
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void canGetExceptionTryingToUnlockFromParallelThread()
                                                                 throws Exception {
        final TransactionModificationContext c1 = TransactionModificationContext.borrowObject();
        final TransactionModificationLock lock = new TransactionModificationLock( true );

        throw JVMUtil.runAndGetExecutionException( new Runnable() {

            @Override
            public void run() {
                lock.unlock( c1.getTransactionId() );
            }
        } );
    }

    @Test
    public void canAcquireLockAfterBeingLocked()
                                                throws InterruptedException {
        final TransactionModificationContext c1 = TransactionModificationContext.borrowObject();
        final TransactionModificationContext c2 = TransactionModificationContext.borrowObject();
        final TransactionModificationLock lock = new TransactionModificationLock( true );
        final AtomicBoolean b = new AtomicBoolean();
        lock.lock( c1.getTransactionId() );

        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    lock.tryLock( c2.getTransactionId(), Long.MAX_VALUE, TimeUnit.MILLISECONDS );
                    b.set( isAlive() );
                }
                catch ( InterruptedException e ) {
                    e.printStackTrace();
                }
            }
        };
        thread.start();
        lock.unlock( c1.getTransactionId() );
        thread.join();

        assertTrue( b.get() );
    }

    @Test
    public void canReenterInNonExclusiveMode() {
        TransactionModificationContext c = TransactionModificationContext.borrowObject();
        TransactionModificationLock lock = new TransactionModificationLock( false );
        lock.lock( c.getTransactionId() );
        lock.lock( c.getTransactionId() );
        lock.unlock( c.getTransactionId() );

        lock.lock( c.getTransactionId() );
        lock.lock( c.getTransactionId() );
        lock.unlock( c.getTransactionId() );
    }

    @Test
    public void cantAcquireLockFromParallelTransactionInNonExclusiveMode()
                                                                          throws InterruptedException {
        TransactionModificationLock lock = new TransactionModificationLock( false );

        lock.lock( 1L );
        boolean b = lock.tryLock( 2L, 1, TimeUnit.MILLISECONDS );
        assertFalse( b );

        lock.unlock( 1L );

        b = lock.tryLock( 2L, Long.MAX_VALUE, TimeUnit.MILLISECONDS );
        assertTrue( b );
    }
}
