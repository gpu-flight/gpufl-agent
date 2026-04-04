package com.gpuflight.agent.model;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LogSourceConfigTest {

    @Test
    void nullFolder_defaultsToDot() {
        LogSourceConfig config = new LogSourceConfig(null, "myapp", null);
        assertEquals(".", config.folder());
    }

    @Test
    void nullFilePrefix_defaultsToGpufl() {
        LogSourceConfig config = new LogSourceConfig("/logs", null, null);
        assertEquals("gpufl", config.filePrefix());
    }

    @Test
    void explicitValues_preserved() {
        LogSourceConfig config = new LogSourceConfig("/var/log", "sass_divergence", null);
        assertEquals("/var/log", config.folder());
        assertEquals("sass_divergence", config.filePrefix());
        assertEquals(List.of("device", "scope", "system"), config.logTypes());
    }
}
