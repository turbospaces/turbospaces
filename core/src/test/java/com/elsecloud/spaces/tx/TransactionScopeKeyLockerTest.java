package com.elsecloud.spaces.tx;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.elsecloud.spaces.EntryKeyLockQuard;

@SuppressWarnings("javadoc")
public class TransactionScopeKeyLockerTest {
    TransactionScopeKeyLocker keyLocker;
    long transactionID;

    @Before
    public void before() {
        keyLocker = new TransactionScopeKeyLocker();
        transactionID = new Random().nextLong();
    }

    @Test
    public void canLockReentrantlyAndUnlock() {
        EntryKeyLockQuard writeLock1 = keyLocker.writeLock( "abc", transactionID, 0, true );
        EntryKeyLockQuard writeLock2 = keyLocker.writeLock( "abc", transactionID, 0, true );

        assertThat( writeLock1, is( writeLock2 ) );
        assertThat( writeLock1.hashCode(), is( writeLock2.hashCode() ) );
        assertThat( writeLock1, is( not( new Object() ) ) );

        Assert.assertTrue( writeLock1 == writeLock2 );

        keyLocker.writeUnlock( writeLock1, transactionID );
    }

    @Test
    public void keyLockNotIdenticalAfterReleasing() {
        EntryKeyLockQuard writeLock1 = keyLocker.writeLock( "abc", transactionID, 0, true );
        keyLocker.writeUnlock( writeLock1, transactionID );

        EntryKeyLockQuard writeLock2 = keyLocker.writeLock( "abc", transactionID + 1, 0, true );

        Assert.assertTrue( writeLock1 != writeLock2 );
        Assert.assertTrue( writeLock1.equals( writeLock2 ) );
        Assert.assertFalse( writeLock1.equals( new Object() ) );
        Assert.assertEquals( writeLock1.hashCode(), writeLock2.hashCode() );
    }

    @Test
    public void cantGetLockForTheSameKeyFromParallelTransaction() {
        keyLocker.writeLock( "abc", transactionID, 0, true );
        assertThat( keyLocker.writeLock( "abc", transactionID + 1, 1, true ), is( nullValue() ) );
        assertThat( keyLocker.writeLock( "abc", transactionID + 2, 0, true ), is( nullValue() ) );
    }

    @Test(expected = IllegalStateException.class)
    public void canGetIllegalStateExceptionWhenTryingToUnlockKeyWhichWasNotAcquiredPreviosly() {
        EntryKeyLockQuard writeLock1 = keyLocker.writeLock( "abc", transactionID, 0, true );
        keyLocker.writeUnlock( writeLock1, transactionID + 1 );
        keyLocker.writeUnlock( mock( EntryKeyLockQuard.class ), transactionID );
    }

    @Test
    public void canDealWithThreadInterruption()
                                               throws InterruptedException {
        keyLocker.writeLock( "abc", transactionID, 0, true );
        Thread t = new Thread() {

            @Override
            public void run() {
                keyLocker.writeLock( "abc", transactionID + 1, Long.MAX_VALUE / 2, true );
            }
        };
        t.start();
        t.interrupt();

        t.join();
    }
}
