package com.gpuflight.com.gpuflight.agent.publisher

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.Properties

class KafkaPublisher(bootstrapServers: String): Publisher {
    private val producer: KafkaProducer<String, String>

    init {
        val props = Properties().apply {
            put("bootstrap.servers", bootstrapServers)
            put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
            put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
            put("compression.type", "snappy")
            put("linger.ms", "100")
        }
        producer = KafkaProducer<String, String>(props)
    }

    override suspend fun publish(topic: String, key: String, message: String) {
        // Switching to IO dispatcher to avoid blocking the main thread
        // even though send() is async, it can block on buffer full.
        withContext(Dispatchers.IO) {
            val record = ProducerRecord(topic, key, message)
            producer.send(record)
        }
    }

    override fun close() {
        producer.flush()
        producer.close()
    }
}