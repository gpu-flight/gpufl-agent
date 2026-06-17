package com.gpuflight.agent.model;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LogSourceConfigTest {

    @Test
    void nullFolder_defaultsToDot() {
        LogSourceConfig config = new LogSourceConfig(null, null);
        assertEquals(".", config.folder());
    }

    @Test
    void nullLogTypes_defaultsToAllChannels() {
        LogSourceConfig config = new LogSourceConfig("/logs", null);
        assertEquals(List.of("device", "scope", "system", "sass"), config.logTypes());
    }

    @Test
    void explicitValues_preserved() {
        LogSourceConfig config = new LogSourceConfig("/var/log", List.of("device"));
        assertEquals("/var/log", config.folder());
        assertEquals(List.of("device"), config.logTypes());
    }
}
