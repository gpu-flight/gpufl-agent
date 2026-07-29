package com.gpuflight.agent.publisher;

import com.gpuflight.agent.config.HttpConfig;
import com.gpuflight.agent.config.JsonSettings;
import com.gpuflight.agent.model.LogWrapper;
import com.gpuflight.agent.model.WindowMetadata;

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
            logConnectionError("HTTP event", e);
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
            System.out.println("[agent] HTTP stream POST starting: url=" + config.streamEndpoint()
                + " session=" + sessionId + " lines=" + ndjsonLines.size() + " gzipBytes=" + body.length);
            return postGzStream(sessionId, body);
        } catch (Exception e) {
            logConnectionError("HTTP stream", e);
            return false;
        }
    }

    @Override
    public boolean publishStreamGz(String sessionId, byte[] gzBody) {
        if (gzBody == null || gzBody.length == 0) {
            return true;
        }
        System.out.println("[agent] HTTP stream POST starting (gz window): url=" + config.streamEndpoint()
            + " session=" + sessionId + " gzipBytes=" + gzBody.length);
        return postGzStream(sessionId, gzBody);
    }

    @Override
    public boolean publishStreamGz(
            String sessionId, WindowMetadata window, byte[] gzBody) {
        if (window == null) {
            return publishStreamGz(sessionId, gzBody);
        }
        if (gzBody == null || gzBody.length == 0) {
            return true;
        }
        System.out.println("[agent] HTTP stream POST starting (window): url="
            + config.streamEndpoint() + " session=" + sessionId
            + " window=" + window.windowId()
            + " sequence=" + window.windowSequence()
            + " gzipBytes=" + gzBody.length);
        return postGzStream(sessionId, gzBody, window);
    }

    /**
     * POST an already-gzipped NDJSON body to the stream endpoint and interpret the
     * status. 2xx (accepted), 409 (already finalized on a restart) and 402 (limit)
     * all advance; anything else is a retryable failure.
     */
    private boolean postGzStream(String sessionId, byte[] gzBody) {
        return postGzStream(sessionId, gzBody, null);
    }

    private boolean postGzStream(
            String sessionId, byte[] gzBody, WindowMetadata window) {
        try {
            String url = config.streamEndpoint();
            var requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/x-ndjson")
                .header("Content-Encoding", "gzip")
                .header("X-GpuFlight-Session-Id", sessionId)
                .header("X-GpuFlight-Hostname", InetAddress.getLocalHost().getHostName())
                .POST(HttpRequest.BodyPublishers.ofByteArray(gzBody));
            if (window != null) {
                requestBuilder
                    .header("X-GpuFlight-Window-Id", window.windowId())
                    .header("X-GpuFlight-Window-Channel", window.channel())
                    .header("X-GpuFlight-Window-Sequence",
                            Long.toString(window.windowSequence()))
                    .header("X-GpuFlight-Window-CRC32",
                            Long.toUnsignedString(window.payloadCrc32()));
            }

            addAuthHeader(requestBuilder);

            var response = client.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
            int sc = response.statusCode();
            if (sc >= 200 && sc <= 299) {
                System.out.println("[agent] HTTP stream POST accepted: status=" + sc + " session=" + sessionId);
                return true;
            }
            if (sc == 409 && window != null
                    && response.body().contains("window_identity_conflict")) {
                // Reusing an immutable UUID for different bytes is corruption,
                // not the legacy "session already finalized" terminal success.
                // Do not advance the cursor or delete the payload.
                System.err.println("[GPUFL] transport window identity conflict - "
                    + "refusing acknowledgement, session=" + sessionId
                    + " window=" + window.windowId());
                return false;
            }
            if (sc == 409) {
                // Session already finalized on the backend - typically this agent
                // re-tailing a finished session after a restart. The data is already
                // stored and re-profiling is required to replace it, so retrying NEVER
                // succeeds. Advance like a 2xx so the tailer finishes instead of
                // re-POSTing forever.
                System.out.println("[agent] HTTP stream POST: session already uploaded (409) - "
                    + "skipping, session=" + sessionId);
                return true;
            }
            if (sc == 402) {
                // GPU/workspace limit exceeded - permanent; retrying won't help.
                System.err.println("[GPUFL] GPU limit exceeded for this workspace - "
                    + "skipping session=" + sessionId + ". " + response.body());
                return true;
            }
            System.out.println("HTTP stream publish failed [" + url + "]: " + sc + " - " + response.body());
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception e) {
            logConnectionError("HTTP stream", e);
            return false;
        }
    }

    @Override
    public boolean publishSessionComplete(String sessionId) {
        try {
            String url = config.endpointFor("session-complete");
            System.out.println("[agent] HTTP session-complete POST starting: url=" + url
                + " session=" + sessionId);

            var requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("X-GpuFlight-Session-Id", sessionId)
                .POST(HttpRequest.BodyPublishers.noBody());

            addAuthHeader(requestBuilder);

            var response = client.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
            int sc = response.statusCode();
            if (sc >= 200 && sc <= 299) {
                System.out.println("[agent] HTTP session-complete accepted: status=" + sc
                    + " session=" + sessionId);
                return true;
            }
            if (sc >= 400 && sc < 500) {
                // Old backend without the endpoint (404), or a client error: nothing
                // to retry — the backend's grace path still finalizes the session.
                System.out.println("[agent] HTTP session-complete not applicable (status=" + sc
                    + ") session=" + sessionId + " - backend grace finalize will apply");
                return true;
            }
            System.out.println("HTTP session-complete failed [" + url + "]: " + sc + " - " + response.body());
            return false;   // 5xx: transient, worth a bounded retry
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception e) {
            logConnectionError("HTTP session-complete", e);
            return false;
        }
    }

    private void addAuthHeader(HttpRequest.Builder requestBuilder) {
        if (config.authToken() != null && !config.authToken().isBlank()) {
            // Environment/config-file values can acquire surrounding CR/LF or
            // whitespace while crossing shells. API keys never contain them,
            // and passing them through makes HttpClient reject the header.
            requestBuilder.header("Authorization", "Bearer " + config.authToken().trim());
        }
    }

    private static void logConnectionError(String operation, Exception error) {
        // HttpClient's IllegalArgumentException includes the rejected header
        // value. Printing the exception/message here can therefore disclose a
        // complete Authorization bearer token. The exception type is enough
        // to distinguish malformed requests from transport failures without
        // putting credentials in logs.
        System.out.println(operation + " connection error ("
            + error.getClass().getSimpleName() + ")");
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
