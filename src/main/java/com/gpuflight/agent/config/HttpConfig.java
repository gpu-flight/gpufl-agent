package com.gpuflight.agent.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public record HttpConfig(
    @JsonProperty("endpointUrl") String endpointUrl,
    @JsonProperty("authToken") String authToken,
    @JsonProperty("timeoutSeconds") long timeoutSeconds
) implements PublisherConfig {
    public HttpConfig {
        if (timeoutSeconds <= 0) timeoutSeconds = 10L;
    }
}
