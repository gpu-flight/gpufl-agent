package com.gpuflight.agent.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record LogSourceConfig(
    @JsonProperty("folder") String folder,
    @JsonProperty("filePrefix") String filePrefix
) {
    public LogSourceConfig {
        if (folder == null) folder = ".";
        if (filePrefix == null) filePrefix = "gpufl";
    }
}
