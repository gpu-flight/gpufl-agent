package com.gpuflight.agent.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ArchiverConfig(
    @JsonProperty("endpoint") String endpoint,
    @JsonProperty("bucket") String bucket,
    @JsonProperty("region") String region,
    @JsonProperty("accessKey") String accessKey,
    @JsonProperty("secretKey") String secretKey,
    @JsonProperty("prefix") String prefix,
    @JsonProperty("deleteAfterUpload") boolean deleteAfterUpload
) {
    public ArchiverConfig {
        if (region == null) region = "nyc3";
        if (prefix == null) prefix = "raw-events/";
    }
}
