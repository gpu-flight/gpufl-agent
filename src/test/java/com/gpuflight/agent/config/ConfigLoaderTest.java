package com.gpuflight.agent.config;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConfigLoaderTest {

    private static final Map<String, String> NO_ENV = Map.of();

    @Test
    void booleanFlags_offByDefault_onViaFlagOrEnv() {
        // default off
        assertFalse(ConfigLoader.parseExitWhenDrained(new String[]{}, NO_ENV));
        assertFalse(ConfigLoader.parseIgnorePreexisting(new String[]{}, NO_ENV));
        assertFalse(ConfigLoader.parsePruneFailed(new String[]{}, NO_ENV));
        assertFalse(ConfigLoader.parseExitIfEmpty(new String[]{}, NO_ENV));

        // on via flag
        assertTrue(ConfigLoader.parsePruneFailed(new String[]{"--prune-failed=1"}, NO_ENV));
        assertTrue(ConfigLoader.parseExitIfEmpty(new String[]{"--exit-if-empty=true"}, NO_ENV));

        // on via env
        assertTrue(ConfigLoader.parseExitWhenDrained(new String[]{}, Map.of("GPUFL_AGENT_EXIT_WHEN_DRAINED", "1")));
        assertTrue(ConfigLoader.parseIgnorePreexisting(new String[]{}, Map.of("GPUFL_AGENT_IGNORE_PREEXISTING", "1")));

        // explicit false / 0 -> off
        assertFalse(ConfigLoader.parsePruneFailed(new String[]{"--prune-failed=false"}, NO_ENV));
        assertFalse(ConfigLoader.parseExitIfEmpty(new String[]{}, Map.of("GPUFL_AGENT_EXIT_IF_EMPTY", "0")));
    }

    @Test
    void resolve_flagBeatsEnvBeatsDefault() {
        assertEquals("flag",
                ConfigLoader.resolve(new String[]{"--host=flag"}, "host", "ENV", "def", Map.of("ENV", "e")));
        assertEquals("e",
                ConfigLoader.resolve(new String[]{}, "host", "ENV", "def", Map.of("ENV", "e")));
        assertEquals("def",
                ConfigLoader.resolve(new String[]{}, "host", "ENV", "def", NO_ENV));
    }

    @Test
    void parseLogTypes_splitsTrimsAndDefaultsToNull() {
        assertNull(ConfigLoader.parseLogTypes(null));
        assertNull(ConfigLoader.parseLogTypes("   "));
        assertEquals(List.of("a", "b"), ConfigLoader.parseLogTypes(" a , b "));
    }
}
