package com.gpuflight.com.gpuflight.agent.config

import kotlinx.serialization.json.Json

object JsonSettings {
    val json = Json {
        ignoreUnknownKeys = true
        isLenient = true
    }
}