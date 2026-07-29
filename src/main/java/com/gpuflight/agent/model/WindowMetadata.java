package com.gpuflight.agent.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Immutable client sidecar for one transport window. */
public record WindowMetadata(
        @JsonProperty("schema_version") int schemaVersion,
        @JsonProperty("type") String type,
        @JsonProperty("window_id") String windowId,
        @JsonProperty("session_id") String sessionId,
        @JsonProperty("channel") String channel,
        @JsonProperty("window_sequence") long windowSequence,
        @JsonProperty("opened_mono_ms") long openedMonoMs,
        @JsonProperty("closed_mono_ms") long closedMonoMs,
        @JsonProperty("created_wall_ms") long createdWallMs,
        @JsonProperty("payload_file") String payloadFile,
        @JsonProperty("payload_bytes") long payloadBytes,
        @JsonProperty("payload_crc32") long payloadCrc32
) {
    public boolean isValidFor(
            String expectedSession, String expectedChannel,
            long expectedSequence, String expectedPayload) {
        return schemaVersion == 1
                && "transport_window".equals(type)
                && windowId != null && !windowId.isBlank()
                && expectedSession.equals(sessionId)
                && expectedChannel.equals(channel)
                && expectedSequence == windowSequence
                && expectedPayload.equals(payloadFile)
                && payloadBytes >= 0;
    }
}
