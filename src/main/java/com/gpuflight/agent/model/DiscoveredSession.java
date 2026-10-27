package com.gpuflight.agent.model;

import java.io.File;
import java.util.List;

/**
 * A session found under a watched folder: the folder, the session_id
 * (the subdir name), the channels found in it to tail, and the --log-types
 * filter they were found under (empty = none), which a later rescan of the
 * session for new channels applies again.
 */
public record DiscoveredSession(
        File folder, String sessionId, List<String> logTypes, List<String> channelFilter) {

    public DiscoveredSession {
        if (channelFilter == null) channelFilter = List.of();
    }

    /** A session limited to {@code logTypes}: a rescan looks for those channels only. */
    public DiscoveredSession(File folder, String sessionId, List<String> logTypes) {
        this(folder, sessionId, logTypes, logTypes);
    }
}
