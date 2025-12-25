package com.gpuflight.com.gpuflight.agent.model

import com.gpuflight.com.gpuflight.agent.config.PublisherConfig
import kotlinx.serialization.Serializable

@Serializable
data class AgentConfig(
    val source: LogSourceConfig,
    val publisher: PublisherConfig
)

@Serializable
data class LogSourceConfig(
    val folder: String = ".",
    val filePrefix: String = "gpufl"
)