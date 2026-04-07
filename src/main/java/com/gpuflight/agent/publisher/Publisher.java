package com.gpuflight.agent.publisher;

import com.gpuflight.agent.model.LogWrapper;

import java.io.Closeable;

public interface Publisher extends Closeable {
    /** Returns true if the event was accepted by the backend, false on failure. */
    boolean publish(String topic, String key, LogWrapper log);
}
