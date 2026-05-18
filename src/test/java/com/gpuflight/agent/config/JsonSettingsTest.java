package com.gpuflight.agent.config;

import tools.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JsonSettingsTest {

    @Test
    void mapper_isNotNull() {
        assertNotNull(JsonSettings.MAPPER);
        assertInstanceOf(ObjectMapper.class, JsonSettings.MAPPER);
    }

    @Test
    void mapper_doesNotFailOnUnknownProperties() throws Exception {
        // FAIL_ON_UNKNOWN_PROPERTIES is disabled — extra fields must be ignored.
        // `aBrandNewField` is a stand-in for any future-version or
        // typoed key. The required fields (hostUrl) ARE present so
        // the HttpConfig compact constructor doesn't fire its
        // migration-error path — this test is solely about Jackson's
        // unknown-field tolerance, not about validation behavior.
        String json = """
            {
              "unknownField": "value",
              "aBrandNewField": 42,
              "type": "http",
              "hostUrl": "http://localhost",
              "authToken": null
            }
            """;
        // Should deserialize without throwing
        HttpConfig config = JsonSettings.MAPPER.readValue(json, HttpConfig.class);
        assertNotNull(config);
        assertEquals("http://localhost", config.hostUrl());
    }

    @Test
    void defaultConstructor_isAccessible() {
        // Covers the implicit default constructor bytecode
        JsonSettings instance = new JsonSettings();
        assertNotNull(instance);
    }
}
