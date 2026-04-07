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
}
