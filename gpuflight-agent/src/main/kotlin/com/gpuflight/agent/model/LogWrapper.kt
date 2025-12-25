package com.gpuflight.com.gpuflight.agent.model

import kotlinx.serialization.json.JsonElement

data class LogWrapper(
    val src: String,
    val timestamp: Long,
    val data: JsonElement
)
