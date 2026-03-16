package com.gpuflight.agent;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CursorState {
    private final Map<String, CursorPosition> streams;

    public CursorState() {
        this.streams = new ConcurrentHashMap<>();
    }

    @JsonCreator
    public CursorState(@JsonProperty("streams") Map<String, CursorPosition> streams) {
        this.streams = streams != null ? new ConcurrentHashMap<>(streams) : new ConcurrentHashMap<>();
    }

    public Map<String, CursorPosition> streams() {
        return streams;
    }
}
