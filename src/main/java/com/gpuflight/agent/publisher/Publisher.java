package com.gpuflight.agent.publisher;

import com.gpuflight.agent.model.LogWrapper;

import java.io.Closeable;
import java.util.List;

public interface Publisher extends Closeable {
    /** Returns true if the event was accepted by the backend, false on failure. */
    boolean publish(String topic, String key, LogWrapper log);

    /** Returns true if the NDJSON batch was accepted by the backend. */
    default boolean publishStream(String sessionId, List<String> ndjsonLines) {
        return false;
    }
}
