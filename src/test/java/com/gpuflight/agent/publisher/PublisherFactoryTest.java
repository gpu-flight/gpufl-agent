package com.gpuflight.agent.publisher;

import com.gpuflight.agent.config.HttpConfig;
import com.gpuflight.agent.config.KafkaConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PublisherFactoryTest {

    @Test
    void create_httpConfig_returnsHttpPublisher() {
        HttpConfig config = new HttpConfig("http://localhost:8080/", null, 5);
        Publisher publisher = PublisherFactory.create(config);
        assertInstanceOf(HttpPublisher.class, publisher);
    }

    @Test
    void create_httpConfig_publisherCloseDoesNotThrow() {
        HttpConfig config = new HttpConfig("http://localhost:8080/", "tok", 10);
        Publisher publisher = PublisherFactory.create(config);
        assertDoesNotThrow(publisher::close);
    }

    @Test
    void create_kafkaConfig_returnsKafkaPublisher() {
        // KafkaProducer constructor is lazy — it doesn't connect until send() is called.
        KafkaConfig config = new KafkaConfig("localhost:9092", "prefix", "snappy");
        Publisher publisher = PublisherFactory.create(config);
        assertInstanceOf(KafkaPublisher.class, publisher);
    }
}
