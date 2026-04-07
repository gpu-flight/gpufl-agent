package com.gpuflight.agent.publisher;

import com.gpuflight.agent.config.JsonSettings;
import com.gpuflight.agent.model.LogWrapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.gpuflight.agent.config.KafkaConfig;

import java.util.Properties;

public class KafkaPublisher implements Publisher {
    private final KafkaProducer<String, String> producer;

    public KafkaPublisher(KafkaConfig config) {
        var props = new Properties();
        props.put("bootstrap.servers", config.bootstrapServers());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("compression.type", config.compression());
        props.put("linger.ms", String.valueOf(config.lingerMs()));
        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public boolean publish(String topic, String key, LogWrapper log) {
        try {
            String message = JsonSettings.MAPPER.writeValueAsString(log.data());
            // producer.send() is async and only blocks when the internal buffer is full.
            // Blocking on a virtual thread is fine — carrier thread parks, not blocks.
            producer.send(new ProducerRecord<>(topic, key, message)).get();
            return true;
        } catch (Exception e) {
            System.out.println("Kafka publish error: " + e.getMessage());
            return false;
        }
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}
