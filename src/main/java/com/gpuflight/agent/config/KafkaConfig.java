package com.gpuflight.agent.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public record KafkaConfig(
    @JsonProperty("bootstrapServers") String bootstrapServers,
    @JsonProperty("topicPrefix") String topicPrefix,
    @JsonProperty("compression") String compression
) implements PublisherConfig {
    public KafkaConfig {
        if (topicPrefix == null) topicPrefix = "gpu-trace";
        if (compression == null) compression = "snappy";
    }
}
