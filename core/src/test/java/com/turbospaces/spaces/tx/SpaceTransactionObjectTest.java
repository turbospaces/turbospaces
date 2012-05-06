package com.turbospaces.spaces.tx;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.turbospaces.spaces.tx.SpaceTransactionHolder;
import com.turbospaces.spaces.tx.SpaceTransactionObject;

@SuppressWarnings("javadoc")
public class SpaceTransactionObjectTest {

    @Test
    public void canSetRollbackOnlyFlag() {
        SpaceTransactionObject object = new SpaceTransactionObject();
        object.setSpaceTransactionHolder( new SpaceTransactionHolder() );
        object.setRollbackOnly();

        assertTrue( object.isRollbackOnly() );
    }
}
