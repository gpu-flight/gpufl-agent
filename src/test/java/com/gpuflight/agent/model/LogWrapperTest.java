package com.gpuflight.agent.model;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class LogWrapperTest {

    @Test
    void testRecordProperties() {
        LogWrapper wrapper = new LogWrapper(123456L, "some data", "log_type", "host1", "127.0.0.1");
        assertEquals(123456L, wrapper.agentSendingTime());
        assertEquals("some data", wrapper.data());
        assertEquals("log_type", wrapper.type());
        assertEquals("host1", wrapper.hostname());
        assertEquals("127.0.0.1", wrapper.ipAddr());
    }

    @Test
    void testEquality() {
        LogWrapper w1 = new LogWrapper(100L, "data", "type", "host", "127.0.0.1");
        LogWrapper w2 = new LogWrapper(100L, "data", "type", "host", "127.0.0.1");
        LogWrapper w3 = new LogWrapper(200L, "data", "type", "host", "127.0.0.1");

        assertEquals(w1, w2);
        assertNotEquals(w1, w3);
        assertEquals(w1.hashCode(), w2.hashCode());
    }

    @Test
    void testToString() {
        LogWrapper wrapper = new LogWrapper(100L, "data", "type", "host", "127.0.0.1");
        String s = wrapper.toString();
        assertTrue(s.contains("agentSendingTime=100"));
        assertTrue(s.contains("data=data"));
        assertTrue(s.contains("type=type"));
    }
}
