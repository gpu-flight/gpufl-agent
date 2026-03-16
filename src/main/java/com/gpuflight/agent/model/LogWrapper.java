package com.gpuflight.agent.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record LogWrapper(
    @JsonProperty("agentSendingTime") long agentSendingTime,
    @JsonProperty("data") String data,
    @JsonProperty("type") String type,
    @JsonProperty("hostname") String hostname,
    @JsonProperty("ipAddr") String ipAddr
) {}
