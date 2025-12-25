package com.gpuflight.com.gpuflight.agent.config

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
sealed interface PublisherConfig

@Serializable
@SerialName("kafka")
data class KafkaConfig(
    val bootstrapServers: String,
    val topicPrefix: String = "gpu-trace",
    val compression: String = "snappt"
): PublisherConfig
@Serializable
@SerialName("http")
data class HttpConfig(
    val endpointUrl: String,
    val authToken: String? = null,
    val timeoutSeconds: Long = 10
) : PublisherConfig