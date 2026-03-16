package com.gpuflight.agent.publisher;

import com.gpuflight.agent.config.HttpConfig;
import com.gpuflight.agent.config.KafkaConfig;
import com.gpuflight.agent.config.PublisherConfig;

public class PublisherFactory {
    public static Publisher create(PublisherConfig config) {
        return switch (config) {
            case KafkaConfig kafka -> new KafkaPublisher(kafka.bootstrapServers());
            case HttpConfig http  -> new HttpPublisher(http);
        };
    }
}
