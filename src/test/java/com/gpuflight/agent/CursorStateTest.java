package com.gpuflight.agent;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CursorStateTest {

    @Test
    void defaultConstructor_createsEmptyMap() {
        CursorState state = new CursorState();
        assertNotNull(state.streams());
        assertTrue(state.streams().isEmpty());
    }

    @Test
    void jsonCreator_withNullMap_createsEmptyMap() {
        CursorState state = new CursorState(null);
        assertNotNull(state.streams());
        assertTrue(state.streams().isEmpty());
    }

    @Test
    void jsonCreator_withPopulatedMap_copiesEntries() {
        Map<String, CursorPosition> input = Map.of(
            "stream1", new CursorPosition(0, 512L),
            "stream2", new CursorPosition(1, 1024L)
        );
        CursorState state = new CursorState(input);
        assertEquals(2, state.streams().size());
        assertEquals(512L, state.streams().get("stream1").offset());
        assertEquals(1, state.streams().get("stream2").fileIndex());
    }

    @Test
    void streams_isMutable() {
        CursorState state = new CursorState();
        state.streams().put("key", new CursorPosition(0, 100L));
        assertEquals(100L, state.streams().get("key").offset());
    }

    @Test
    void jsonCreator_withEmptyMap_works() {
        CursorState state = new CursorState(Map.of());
        assertNotNull(state.streams());
        assertTrue(state.streams().isEmpty());
    }
}
