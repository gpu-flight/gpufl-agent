package com.gpuflight.agent.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HttpConfigTest {

    @Test
    void zeroTimeout_defaultsToTen() {
        HttpConfig config = new HttpConfig("http://localhost", "v1", null, 0);
        assertEquals(10L, config.timeoutSeconds());
    }

    @Test
    void negativeTimeout_defaultsToTen() {
        HttpConfig config = new HttpConfig("http://localhost", "v1", "tok", -5);
        assertEquals(10L, config.timeoutSeconds());
    }

    @Test
    void positiveTimeout_preserved() {
        HttpConfig config = new HttpConfig("http://localhost", "v1", null, 30);
        assertEquals(30L, config.timeoutSeconds());
    }

    @Test
    void fields_accessible() {
        HttpConfig config = new HttpConfig("http://example.com", "v1", "mytoken", 15);
        assertEquals("http://example.com", config.hostUrl());
        assertEquals("v1", config.apiVersion());
        assertEquals("mytoken", config.authToken());
    }

    @Test
    void apiVersion_defaultsToV1WhenNull() {
        HttpConfig config = new HttpConfig("http://localhost", null, null, 5);
        assertEquals("v1", config.apiVersion());
    }

    @Test
    void apiVersion_defaultsToV1WhenBlank() {
        HttpConfig config = new HttpConfig("http://localhost", "   ", null, 5);
        assertEquals("v1", config.apiVersion());
    }

    @Test
    void hostUrl_trailingSlashStripped() {
        HttpConfig config = new HttpConfig("http://localhost:8080/", "v1", null, 5);
        assertEquals("http://localhost:8080", config.hostUrl());
    }

    @Test
    void endpointFor_buildsCanonicalUrl() {
        HttpConfig config = new HttpConfig("https://api.gpuflight.com", "v1", null, 5);
        assertEquals("https://api.gpuflight.com/api/v1/events/init",   config.endpointFor("init"));
        assertEquals("https://api.gpuflight.com/api/v1/events/kernel", config.endpointFor("kernel"));
    }

    @Test
    void endpointFor_honorsApiVersion() {
        HttpConfig config = new HttpConfig("https://api.gpuflight.com", "v2", null, 5);
        assertEquals("https://api.gpuflight.com/api/v2/events/init", config.endpointFor("init"));
    }

    @Test
    void hostUrl_nullRejected_withMigrationMessage() {
        // The compact constructor should fail loudly when hostUrl is
        // missing — this is the failure mode a stale config with the
        // legacy `endpointUrl` field hits (Jackson silently sets the
        // unknown new field to null). The message must mention the
        // migration so users can self-recover without grepping source.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
            () -> new HttpConfig(null, "v1", null, 5));
        assertTrue(ex.getMessage().contains("hostUrl"),
                "Error message should name the required field");
        assertTrue(ex.getMessage().contains("endpointUrl"),
                "Error message should mention the legacy field name for migration discoverability");
    }

    @Test
    void hostUrl_blankRejected() {
        assertThrows(IllegalArgumentException.class,
            () -> new HttpConfig("   ", "v1", null, 5));
    }
}
