package com.gpuflight.agent.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Upload-mode + stream-batch fields on HttpConfig. */
class HttpConfigUploadModeTest {

    @Test
    void defaults_streamModeWithBatchLimits() {
        HttpConfig c = new HttpConfig("https://api.gpuflight.com", "v1", null, 10,
                null, 0, 0L);
        assertEquals("stream", c.uploadMode());
        assertTrue(c.isStreamMode());
        assertEquals(HttpConfig.DEFAULT_STREAM_MAX_LINES, c.streamMaxLines());
        assertEquals(HttpConfig.DEFAULT_STREAM_MAX_BYTES, c.streamMaxBytes());
        assertEquals("https://api.gpuflight.com/api/v1/events/stream", c.streamEndpoint());
    }

    @Test
    void legacyFourFieldConstructor_getsStreamDefaults() {
        HttpConfig c = new HttpConfig("https://api.gpuflight.com", "v1", null, 10);
        assertTrue(c.isStreamMode());
        assertEquals(HttpConfig.DEFAULT_STREAM_MAX_LINES, c.streamMaxLines());
    }

    @Test
    void legacyMode_disablesStream() {
        HttpConfig c = new HttpConfig("https://api.gpuflight.com", "v1", null, 10,
                "legacy", 0, 0L);
        assertFalse(c.isStreamMode());
        assertEquals(StreamUploadSettings.DISABLED, StreamUploadSettings.from(c));
    }

    @Test
    void streamMode_settingsCarryLimits() {
        HttpConfig c = new HttpConfig("https://api.gpuflight.com", "v1", null, 10,
                "stream", 250, 1_000_000L);
        StreamUploadSettings s = StreamUploadSettings.from(c);
        assertTrue(s.enabled());
        assertEquals(250, s.maxLines());
        assertEquals(1_000_000L, s.maxBytes());
    }

    @Test
    void unknownUploadMode_rejectedAtConstruction() {
        assertThrows(IllegalArgumentException.class, () ->
                new HttpConfig("https://api.gpuflight.com", "v1", null, 10,
                        "carrier-pigeon", 0, 0L));
    }
}
