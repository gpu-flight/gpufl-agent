package com.gpuflight.agent;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CursorPosition(
    @JsonProperty("fileIndex") int fileIndex,
    @JsonProperty("offset") long offset
) {}
