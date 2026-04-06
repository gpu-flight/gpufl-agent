package com.gpuflight.agent.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gpuflight.agent.config.PublisherConfig;

import java.util.List;

public record AgentConfig(
    @JsonProperty("source") LogSourceConfig source,
    @JsonProperty("sources") List<LogSourceConfig> sources,
    @JsonProperty("publisher") PublisherConfig publisher,
    @JsonProperty("archiver") ArchiverConfig archiver
) {
    /**
     * Returns all configured sources. Supports both the legacy single
     * {@code "source"} field and the new {@code "sources"} array.
     * If both are set, they are merged (source first, then sources).
     */
    public List<LogSourceConfig> allSources() {
        var list = new java.util.ArrayList<LogSourceConfig>();
        if (source != null) list.add(source);
        if (sources != null) list.addAll(sources);
        return list;
    }
}
