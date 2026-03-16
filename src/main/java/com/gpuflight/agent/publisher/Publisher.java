package com.gpuflight.agent.publisher;

import com.gpuflight.agent.model.LogWrapper;

import java.io.Closeable;

public interface Publisher extends Closeable {
    void publish(String topic, String key, LogWrapper log);
}
