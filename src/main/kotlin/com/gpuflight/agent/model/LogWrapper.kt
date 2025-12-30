package com.gpuflight.com.gpuflight.agent.model

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement

@Serializable
data class LogWrapper(
    val agentSendingTime: Long,
    val data: String,
    val type: String,
    val hostname: String,
    val ipAddr: String
)
