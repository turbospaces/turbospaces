package com.turbospaces.spaces;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.turbospaces.api.JSpace;
import com.turbospaces.spaces.SpaceModifiers;

@SuppressWarnings("javadoc")
public class SpaceModifiersTest {

    @Test
    public void identifiesWriteOnlyModifierCorrectly() {
        assertTrue( SpaceModifiers.isWriteOnly( JSpace.WRITE_ONLY ) );
        assertFalse( SpaceModifiers.isWriteOnly( JSpace.WRITE_OR_UPDATE ) );
        assertFalse( SpaceModifiers.isWriteOnly( JSpace.UPDATE_ONLY ) );
    }

    @Test
    public void identifiesWriteOrUpdateModifierCorrectly() {
        assertTrue( SpaceModifiers.isWriteOrUpdate( JSpace.WRITE_OR_UPDATE ) );
        assertFalse( SpaceModifiers.isWriteOrUpdate( JSpace.WRITE_ONLY ) );
        assertFalse( SpaceModifiers.isWriteOnly( JSpace.UPDATE_ONLY ) );
    }

    @Test
    public void identifiesUpdateOnlyModifierCorrectly() {
        assertTrue( SpaceModifiers.isUpdateOnly( JSpace.UPDATE_ONLY ) );
        assertFalse( SpaceModifiers.isUpdateOnly( JSpace.WRITE_ONLY ) );
        assertFalse( SpaceModifiers.isUpdateOnly( JSpace.WRITE_OR_UPDATE ) );
    }

    @Test
    public void identifiesEvictOnlyModifierCorrectly() {
        assertTrue( SpaceModifiers.isEvictOnly( JSpace.EVICT_ONLY | JSpace.MATCH_BY_ID ) );
        assertTrue( SpaceModifiers.isMatchById( JSpace.EVICT_ONLY | JSpace.MATCH_BY_ID ) );
        assertFalse( SpaceModifiers.isEvictOnly( JSpace.MATCH_BY_ID | JSpace.WRITE_OR_UPDATE ) );
        assertFalse( SpaceModifiers.isMatchById( JSpace.EVICT_ONLY | JSpace.UPDATE_ONLY ) );
        assertFalse( SpaceModifiers.isEvictOnly( JSpace.MATCH_BY_ID | JSpace.WRITE_OR_UPDATE ) );
    }

    @Test
    public void identifiesReadOnlyModifierCorrectly() {
        assertTrue( SpaceModifiers.isReadOnly( JSpace.READ_ONLY | JSpace.MATCH_BY_ID ) );
        assertTrue( SpaceModifiers.isMatchById( JSpace.READ_ONLY | JSpace.MATCH_BY_ID ) );
        assertFalse( SpaceModifiers.isReadOnly( JSpace.MATCH_BY_ID | JSpace.TAKE_ONLY ) );
    }

    @Test
    public void identifiesTakeOnlyModifierCorrectly() {
        assertTrue( SpaceModifiers.isTakeOnly( JSpace.TAKE_ONLY | JSpace.MATCH_BY_ID ) );
        assertTrue( SpaceModifiers.isMatchById( JSpace.TAKE_ONLY | JSpace.MATCH_BY_ID ) );
        assertFalse( SpaceModifiers.isTakeOnly( JSpace.MATCH_BY_ID | JSpace.EVICT_ONLY ) );
    }

    @Test
    public void identifiesExclusiveReadLockCorrectly() {
        assertTrue( SpaceModifiers.isExclusiveRead( JSpace.EXCLUSIVE_READ_LOCK | JSpace.MATCH_BY_ID | JSpace.READ_ONLY ) );
        assertTrue( SpaceModifiers.isMatchById( JSpace.EXCLUSIVE_READ_LOCK | JSpace.MATCH_BY_ID ) );
        assertFalse( SpaceModifiers.isExclusiveRead( JSpace.MATCH_BY_ID | JSpace.READ_ONLY ) );
    }

    @Test
    public void identifiesReturnAsBytesCorrectly() {
        assertTrue( SpaceModifiers.isReturnAsBytes( JSpace.EXCLUSIVE_READ_LOCK | JSpace.MATCH_BY_ID | JSpace.RETURN_AS_BYTES ) );
        assertFalse( SpaceModifiers.isReturnAsBytes( JSpace.EXCLUSIVE_READ_LOCK | JSpace.MATCH_BY_ID ) );
    }
}
