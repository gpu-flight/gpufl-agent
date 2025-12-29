package com.gpuflight.com.gpuflight.agent.publisher

import com.gpuflight.com.gpuflight.agent.model.LogWrapper
import java.io.Closeable

interface Publisher : Closeable {
    suspend fun publish(topic: String, key: String, log: LogWrapper)
}