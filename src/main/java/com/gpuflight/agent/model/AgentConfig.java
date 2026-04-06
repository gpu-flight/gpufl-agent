package com.gpuflight.agent.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gpuflight.agent.config.PublisherConfig;

public record AgentConfig(
    @JsonProperty("source") LogSourceConfig source,
    @JsonProperty("publisher") PublisherConfig publisher,
    @JsonProperty("archiver") ArchiverConfig archiver
) {}
