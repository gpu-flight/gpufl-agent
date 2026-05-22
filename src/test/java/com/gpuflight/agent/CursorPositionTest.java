package com.gpuflight.agent;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CursorPositionTest {

    @Test
    void testRecordProperties() {
        CursorPosition pos = new CursorPosition(1, 100L);
        assertEquals(1, pos.fileIndex());
        assertEquals(100L, pos.offset());
    }

    @Test
    void testEquality() {
        CursorPosition pos1 = new CursorPosition(1, 100L);
        CursorPosition pos2 = new CursorPosition(1, 100L);
        CursorPosition pos3 = new CursorPosition(2, 100L);
        CursorPosition pos4 = new CursorPosition(1, 200L);

        assertEquals(pos1, pos2);
        assertNotEquals(pos1, pos3);
        assertNotEquals(pos1, pos4);
        assertEquals(pos1.hashCode(), pos2.hashCode());
    }

    @Test
    void testToString() {
        CursorPosition pos = new CursorPosition(1, 100L);
        String s = pos.toString();
        assertTrue(s.contains("fileIndex=1"));
        assertTrue(s.contains("offset=100"));
    }

    @Test
    void testIdentityDefaultsForLegacyConstructor() {
        // The 2-arg constructor (legacy call sites + old cursor files) must leave
        // identity unset so behavior degrades to the original logic.
        CursorPosition pos = new CursorPosition(1, 100L);
        assertNull(pos.fileKey());
        assertEquals(0L, pos.fileSize());
        assertEquals(0L, pos.headSig());
    }

    @Test
    void testFullConstructorCarriesIdentity() {
        CursorPosition pos = new CursorPosition(2, 200L, "inode-123", 4096L, 99887766L);
        assertEquals(2, pos.fileIndex());
        assertEquals(200L, pos.offset());
        assertEquals("inode-123", pos.fileKey());
        assertEquals(4096L, pos.fileSize());
        assertEquals(99887766L, pos.headSig());
    }
}
