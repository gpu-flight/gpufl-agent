package com.gpuflight.agent.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LogSourceConfigTest {

    @Test
    void nullFolder_defaultsToDot() {
        LogSourceConfig config = new LogSourceConfig(null, "myapp");
        assertEquals(".", config.folder());
    }

    @Test
    void nullFilePrefix_defaultsToGpufl() {
        LogSourceConfig config = new LogSourceConfig("/logs", null);
        assertEquals("gpufl", config.filePrefix());
    }

    @Test
    void explicitValues_preserved() {
        LogSourceConfig config = new LogSourceConfig("/var/log", "sass_divergence");
        assertEquals("/var/log", config.folder());
        assertEquals("sass_divergence", config.filePrefix());
    }
}
