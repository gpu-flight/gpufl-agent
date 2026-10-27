package com.gpuflight.agent.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gpuflight.agent.config.ConfigLoader;

import java.util.List;

/**
 * A folder to watch for gpufl sessions. In v1.2 each run writes its logs
 * under {@code <folder>/<session_id>/<channel>.log[.gz]}, so a source is
 * just a folder — sessions are auto-discovered (the {@code session_id} is the
 * subdir name). {@code logTypes} optionally restricts which channels to tail;
 * without it every channel found in a session is tailed.
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
        // No filter by default: every channel a producer publishes is tailed, including
        // "sass" (the SASS-disassembly / source-content artifacts split out of device.log)
        // and "serving"; a fixed list would miss a producer's new channel.
        if (logTypes == null || logTypes.isEmpty()) logTypes = ConfigLoader.DEFAULT_LOG_TYPES;
    }
}
