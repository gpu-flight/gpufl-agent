package com.gpuflight.agent.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * A folder to watch for gpufl sessions. In v1.2 each run writes its logs
 * under {@code <folder>/<session_id>/<channel>.log[.gz]}, so a source is
 * just a folder — sessions are auto-discovered (the {@code session_id} is the
 * subdir name). {@code logTypes} optionally restricts which channels to tail.
 *
 * <p>The pre-v1.2 {@code filePrefix} field was removed: it no longer maps to
 * anything on disk (sessions are nested, not flat {@code <prefix>.<channel>}),
 * and leaving it in the config only confused users.
 */
public record LogSourceConfig(
    @JsonProperty("folder")   String folder,
    @JsonProperty("logTypes") List<String> logTypes
) {
    public LogSourceConfig {
        if (folder == null) folder = ".";
        // "sass" carries the SASS-disassembly / source-content artifacts split
        // out of device.log; tail it by default or they miss live upload.
        if (logTypes == null || logTypes.isEmpty()) logTypes = List.of("device", "scope", "system", "sass");
    }
}
