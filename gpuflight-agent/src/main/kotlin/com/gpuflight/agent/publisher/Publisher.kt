package com.gpuflight.com.gpuflight.agent.publisher

import java.io.Closeable

interface Publisher : Closeable {
    suspend fun publish(topic: String, key: String, message: String)
}