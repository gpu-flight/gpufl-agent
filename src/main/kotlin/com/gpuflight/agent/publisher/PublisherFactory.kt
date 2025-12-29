package com.gpuflight.com.gpuflight.agent.publisher

import com.gpuflight.com.gpuflight.agent.config.HttpConfig
import com.gpuflight.com.gpuflight.agent.config.KafkaConfig
import com.gpuflight.com.gpuflight.agent.config.PublisherConfig

object PublisherFactory {

    fun create(config: PublisherConfig): Publisher {
        return when (config) {
            is KafkaConfig -> KafkaPublisher(config.bootstrapServers)
            is HttpConfig -> HttpPublisher(config)
        }
    }
}