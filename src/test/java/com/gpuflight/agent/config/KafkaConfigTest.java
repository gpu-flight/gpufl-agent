package com.gpuflight.agent.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KafkaConfigTest {

    @Test
    void nullTopicPrefix_defaultsToGpuTrace() {
        KafkaConfig config = new KafkaConfig("localhost:9092", null, null);
        assertEquals("gpu-trace", config.topicPrefix());
    }

    @Test
    void nullCompression_defaultsToSnappy() {
        KafkaConfig config = new KafkaConfig("localhost:9092", null, null);
        assertEquals("snappy", config.compression());
    }

    @Test
    void explicitValues_notOverridden() {
        KafkaConfig config = new KafkaConfig("b1:9092,b2:9092", "my-prefix", "lz4");
        assertEquals("b1:9092,b2:9092", config.bootstrapServers());
        assertEquals("my-prefix", config.topicPrefix());
        assertEquals("lz4", config.compression());
    }
}
