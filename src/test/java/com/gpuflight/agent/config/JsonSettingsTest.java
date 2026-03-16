package com.gpuflight.agent.config;

import com.fasterxml.jackson.databind.ObjectMapper;
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
        // FAIL_ON_UNKNOWN_PROPERTIES is disabled — extra fields must be ignored
        String json = "{\"unknownField\":\"value\",\"type\":\"http\",\"endpointUrl\":\"http://localhost/\",\"authToken\":null}";
        // Should deserialize without throwing
        HttpConfig config = JsonSettings.MAPPER.readValue(json, HttpConfig.class);
        assertNotNull(config);
    }

    @Test
    void defaultConstructor_isAccessible() {
        // Covers the implicit default constructor bytecode
        JsonSettings instance = new JsonSettings();
        assertNotNull(instance);
    }
}
