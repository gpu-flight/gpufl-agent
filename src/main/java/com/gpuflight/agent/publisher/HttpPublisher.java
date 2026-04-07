package com.gpuflight.agent.publisher;

import com.gpuflight.agent.config.HttpConfig;
import com.gpuflight.agent.config.JsonSettings;
import com.gpuflight.agent.model.LogWrapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class HttpPublisher implements Publisher {
    private final HttpConfig config;
    private final HttpClient client;

    public HttpPublisher(HttpConfig config) {
        this.config = config;
        this.client = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(config.timeoutSeconds()))
            .build();
    }

    @Override
    public boolean publish(String topic, String key, LogWrapper log) {
        try {
            String url = config.endpointUrl() + log.type();
            String message = JsonSettings.MAPPER.writeValueAsString(log);

            var requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .header("X-Topic", topic)
                .header("X-Key", key)
                .POST(HttpRequest.BodyPublishers.ofString(message));

            if (config.authToken() != null) {
                requestBuilder.header("Authorization", "Bearer " + config.authToken());
            }

            // Blocking send is fine on a virtual thread — the carrier thread parks, not blocks.
            var response = client.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 200 && response.statusCode() <= 299) {
                return true;
            }
            System.out.println("HTTP Publish failed [" + url + "]: " + response.statusCode() + " - " + response.body());
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception e) {
            System.out.println("HTTP Connection error: " + e);
            return false;
        }
    }

    @Override
    public void close() {
        // Java HttpClient has no explicit close; resources are reclaimed on GC.
    }
}
