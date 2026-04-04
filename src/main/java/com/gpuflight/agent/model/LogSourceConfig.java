package com.gpuflight.agent.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record LogSourceConfig(
    @JsonProperty("folder")   String folder,
    @JsonProperty("filePrefix") String filePrefix,
    @JsonProperty("logTypes") List<String> logTypes
) {
    public LogSourceConfig {
        if (folder == null) folder = ".";
        if (filePrefix == null) filePrefix = "gpufl";
        if (logTypes == null || logTypes.isEmpty()) logTypes = List.of("device", "scope", "system");
    }
}
