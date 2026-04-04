package com.gpuflight.agent.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public record KafkaConfig(
    @JsonProperty("bootstrapServers") String bootstrapServers,
    @JsonProperty("topicPrefix")      String topicPrefix,
    @JsonProperty("compression")      String compression,
    @JsonProperty("lingerMs")         int lingerMs
) implements PublisherConfig {
    public KafkaConfig {
        if (topicPrefix == null) topicPrefix = "gpu-trace";
        if (compression == null) compression = "snappy";
        if (lingerMs <= 0) lingerMs = 100;
    }
}
