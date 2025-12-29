package com.gpuflight.com.gpuflight.agent.model

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement

@Serializable
data class LogWrapper(
    val src: String,
    val timestamp: Long,
    val data: JsonElement,
    val type: String
)
