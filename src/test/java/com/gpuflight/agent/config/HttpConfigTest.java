package com.gpuflight.agent.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HttpConfigTest {

    @Test
    void zeroTimeout_defaultsToTen() {
        HttpConfig config = new HttpConfig("http://localhost/", null, 0);
        assertEquals(10L, config.timeoutSeconds());
    }

    @Test
    void negativeTimeout_defaultsToTen() {
        HttpConfig config = new HttpConfig("http://localhost/", "tok", -5);
        assertEquals(10L, config.timeoutSeconds());
    }

    @Test
    void positiveTimeout_preserved() {
        HttpConfig config = new HttpConfig("http://localhost/", null, 30);
        assertEquals(30L, config.timeoutSeconds());
    }

    @Test
    void fields_accessible() {
        HttpConfig config = new HttpConfig("http://example.com/api/", "mytoken", 15);
        assertEquals("http://example.com/api/", config.endpointUrl());
        assertEquals("mytoken", config.authToken());
    }
}
