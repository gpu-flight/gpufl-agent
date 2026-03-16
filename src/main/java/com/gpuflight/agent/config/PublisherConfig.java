package com.gpuflight.agent.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = KafkaConfig.class, name = "kafka"),
    @JsonSubTypes.Type(value = HttpConfig.class, name = "http")
})
public sealed interface PublisherConfig permits KafkaConfig, HttpConfig {}
