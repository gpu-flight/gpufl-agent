package com.gpuflight.agent.publisher;

import com.gpuflight.agent.config.HttpConfig;
import com.gpuflight.agent.config.JsonSettings;
import com.gpuflight.agent.model.LogWrapper;

import java.io.ByteArrayOutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.zip.GZIPOutputStream;

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
            // Path is structural - assembled inside HttpConfig from
            // hostUrl + apiVersion + event-type segment. See
            // HttpConfig.endpointFor() for the template.
            String url = config.endpointFor(log.type());
            String message = JsonSettings.MAPPER.writeValueAsString(log);
            System.out.println("[agent] HTTP event POST starting: url=" + url
                + " topic=" + topic
                + " key=" + key
                + " type=" + log.type());

            var requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .header("X-Topic", topic)
                .header("X-Key", key)
                .POST(HttpRequest.BodyPublishers.ofString(message));

            addAuthHeader(requestBuilder);

            // Blocking send is fine on a virtual thread - the carrier thread parks, not blocks.
            var response = client.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 200 && response.statusCode() <= 299) {
                System.out.println("[agent] HTTP event POST accepted: status="
                    + response.statusCode() + " type=" + log.type());
                return true;
            }
            if (response.statusCode() == 402) {
                // GPU limit exceeded - permanent failure, do not retry
                System.err.println("[GPUFL] ========================================");
                System.err.println("[GPUFL] GPU limit exceeded for this workspace.");
                System.err.println("[GPUFL] " + response.body());
                System.err.println("[GPUFL] Upgrade at https://gpuflight.com/pricing");
                System.err.println("[GPUFL] ========================================");
                return true; // advance cursor - retrying won't help
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
    public boolean publishStream(String sessionId, List<String> ndjsonLines) {
        if (ndjsonLines == null || ndjsonLines.isEmpty()) {
            return true;
        }
        try {
            String ndjson = String.join("\n", ndjsonLines) + "\n";
            byte[] body = gzip(ndjson.getBytes(StandardCharsets.UTF_8));
            String url = config.streamEndpoint();
            System.out.println("[agent] HTTP stream POST starting: url=" + url
                + " session=" + sessionId
                + " lines=" + ndjsonLines.size()
                + " gzipBytes=" + body.length);

            var requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/x-ndjson")
                .header("Content-Encoding", "gzip")
                .header("X-GpuFlight-Session-Id", sessionId)
                .header("X-GpuFlight-Hostname", InetAddress.getLocalHost().getHostName())
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));

            addAuthHeader(requestBuilder);

            var response = client.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 200 && response.statusCode() <= 299) {
                System.out.println("[agent] HTTP stream POST accepted: status="
                    + response.statusCode() + " session=" + sessionId);
                return true;
            }
            System.out.println("HTTP stream publish failed [" + url + "]: "
                + response.statusCode() + " - " + response.body());
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception e) {
            System.out.println("HTTP stream connection error: " + e);
            return false;
        }
    }

    private void addAuthHeader(HttpRequest.Builder requestBuilder) {
        if (config.authToken() != null && !config.authToken().isBlank()) {
            requestBuilder.header("Authorization", "Bearer " + config.authToken());
        }
    }

    private static byte[] gzip(byte[] content) throws Exception {
        var out = new ByteArrayOutputStream();
        try (var gzip = new GZIPOutputStream(out)) {
            gzip.write(content);
        }
        return out.toByteArray();
    }

    @Override
    public void close() {
        // Java HttpClient has no explicit close; resources are reclaimed on GC.
    }
}
