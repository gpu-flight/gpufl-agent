package com.gpuflight.agent.model;

import java.io.File;
import java.util.List;

/**
 * A session found under a watched folder: the folder, the session_id
 * (the subdir name), and the channels to tail.
 */
public record DiscoveredSession(File folder, String sessionId, List<String> logTypes) {}
