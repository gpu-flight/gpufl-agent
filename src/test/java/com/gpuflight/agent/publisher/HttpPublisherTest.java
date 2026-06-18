package com.gpuflight.agent.publisher;

import com.gpuflight.agent.config.HttpConfig;
import com.gpuflight.agent.model.LogWrapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.*;

class HttpPublisherTest {

    private HttpServer server;
    private int port;
    private final AtomicInteger statusToReturn = new AtomicInteger(200);
    private final AtomicReference<String> lastBody = new AtomicReference<>();
    private final AtomicReference<String> lastAuthHeader = new AtomicReference<>();
    private final AtomicReference<String> lastContentEncoding = new AtomicReference<>();
    private final AtomicReference<String> lastContentType = new AtomicReference<>();
    private final AtomicReference<String> lastPath = new AtomicReference<>();
    private final AtomicReference<String> lastSessionId = new AtomicReference<>();

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/", exchange -> {
            var headers = exchange.getRequestHeaders();
            byte[] body = exchange.getRequestBody().readAllBytes();
            String encoding = headers.getFirst("Content-Encoding");
            lastBody.set("gzip".equalsIgnoreCase(encoding)
                ? gunzip(body)
                : new String(body, StandardCharsets.UTF_8));
            lastAuthHeader.set(headers.getFirst("Authorization"));
            lastContentEncoding.set(encoding);
            lastContentType.set(headers.getFirst("Content-Type"));
            lastPath.set(exchange.getRequestURI().getPath());
            lastSessionId.set(headers.getFirst("X-GpuFlight-Session-Id"));
            exchange.sendResponseHeaders(statusToReturn.get(), 0);
            exchange.close();
        });
        server.start();
        port = server.getAddress().getPort();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    /**
     * Host base for the test fixture. The stub HttpServer above listens
     * on the {@code /api/} context, which matches by prefix — so the
     * full URL built by HttpConfig.endpointFor() (e.g.
     * {@code http://localhost:<port>/api/v1/events/kernel_event}) lands
     * in the same handler that captures the body for the assertions.
     */
    private String hostUrl() {
        return "http://localhost:" + port;
    }

    private LogWrapper sampleLog(String type) {
        return new LogWrapper(System.currentTimeMillis(), "{\"key\":\"val\"}", type, "testhost", "127.0.0.1");
    }

    private static String gunzip(byte[] body) throws IOException {
        try (var gzip = new GZIPInputStream(new ByteArrayInputStream(body))) {
            return new String(gzip.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    void publish_sendsPostRequest() throws InterruptedException {
        HttpConfig config = new HttpConfig(hostUrl(), "v1", null, 5);
        HttpPublisher pub = new HttpPublisher(config);

        pub.publish("topic", "device", sampleLog("kernel_event"));

        // Give the request time to arrive
        Thread.sleep(300);
        assertNotNull(lastBody.get(), "Server should have received a request body");
        assertTrue(lastBody.get().contains("kernel_event"));
    }

    @Test
    void publish_sendsAuthorizationHeader() throws InterruptedException {
        HttpConfig config = new HttpConfig(hostUrl(), "v1", "gpfl_tok123", 5);
        HttpPublisher pub = new HttpPublisher(config);

        pub.publish("topic", "scope", sampleLog("scope_event"));
        Thread.sleep(300);

        assertEquals("Bearer gpfl_tok123", lastAuthHeader.get());
    }

    @Test
    void publish_noToken_doesNotSendAuthHeader() throws InterruptedException {
        HttpConfig config = new HttpConfig(hostUrl(), "v1", null, 5);
        HttpPublisher pub = new HttpPublisher(config);

        pub.publish("topic", "system", sampleLog("system_event"));
        Thread.sleep(300);

        assertNull(lastAuthHeader.get());
    }

    @Test
    void publish_blankToken_doesNotSendAuthHeader() {
        HttpConfig config = new HttpConfig(hostUrl(), "v1", "", 5);
        HttpPublisher pub = new HttpPublisher(config);

        pub.publish("topic", "system", sampleLog("system_event"));

        assertNull(lastAuthHeader.get());
    }

    @Test
    void publishStream_sendsGzippedNdjsonBatch() {
        statusToReturn.set(202);
        HttpConfig config = new HttpConfig(hostUrl(), "v1", null, 5,
                "stream", 100, 1_000_000L);
        HttpPublisher pub = new HttpPublisher(config);

        boolean ok = pub.publishStream("session-1", List.of(
            "{\"type\":\"kernel_event\",\"session_id\":\"session-1\"}",
            "{\"type\":\"scope_event\",\"session_id\":\"session-1\"}"));

        assertTrue(ok);
        assertEquals("/api/v1/events/stream", lastPath.get());
        assertEquals("gzip", lastContentEncoding.get());
        assertEquals("application/x-ndjson", lastContentType.get());
        assertEquals("session-1", lastSessionId.get());
        assertTrue(lastBody.get().contains("\"kernel_event\""));
        assertTrue(lastBody.get().endsWith("\n"));
    }

    @Test
    void publishStream_alreadyUploaded409_advancesAsAccepted() {
        // 409 "session already uploaded" is terminal - retrying never succeeds.
        // publishStream must return true (advance) so the tailer drains to EOF and
        // exits instead of re-POSTing the same batch every 5s forever.
        statusToReturn.set(409);
        HttpConfig config = new HttpConfig(hostUrl(), "v1", null, 5, "stream", 100, 1_000_000L);
        HttpPublisher pub = new HttpPublisher(config);

        boolean ok = pub.publishStream("session-1",
            List.of("{\"type\":\"kernel_event\",\"session_id\":\"session-1\"}"));

        assertTrue(ok, "409 already-uploaded must advance the cursor, not retry forever");
    }

    @Test
    void publishStream_limitExceeded402_advancesAsAccepted() {
        // 402 (workspace/GPU limit) is permanent; retrying won't help.
        statusToReturn.set(402);
        HttpConfig config = new HttpConfig(hostUrl(), "v1", null, 5, "stream", 100, 1_000_000L);
        HttpPublisher pub = new HttpPublisher(config);

        boolean ok = pub.publishStream("session-1",
            List.of("{\"type\":\"kernel_event\",\"session_id\":\"session-1\"}"));

        assertTrue(ok, "402 limit-exceeded must advance, not retry forever");
    }

    @Test
    void publishStream_serverError500_signalsRetry() {
        // A transient 5xx must remain retriable (return false) so the tailer backs
        // off and retries from the same offset.
        statusToReturn.set(500);
        HttpConfig config = new HttpConfig(hostUrl(), "v1", null, 5, "stream", 100, 1_000_000L);
        HttpPublisher pub = new HttpPublisher(config);

        boolean ok = pub.publishStream("session-1",
            List.of("{\"type\":\"kernel_event\",\"session_id\":\"session-1\"}"));

        assertFalse(ok, "5xx is transient and must be retried");
    }

    @Test
    void publish_nonSuccessResponse_doesNotThrow() throws InterruptedException {
        statusToReturn.set(500);
        HttpConfig config = new HttpConfig(hostUrl(), "v1", null, 5);
        HttpPublisher pub = new HttpPublisher(config);

        // Should log the error but not throw
        assertDoesNotThrow(() -> pub.publish("topic", "device", sampleLog("kernel_event")));
        Thread.sleep(300);
    }

    @Test
    void publish_unreachableServer_doesNotThrow() {
        HttpConfig config = new HttpConfig("http://localhost:1", "v1", null, 1);
        HttpPublisher pub = new HttpPublisher(config);

        // Should swallow the connection error
        assertDoesNotThrow(() -> pub.publish("topic", "device", sampleLog("kernel_event")));
    }

    @Test
    void close_doesNotThrow() {
        HttpConfig config = new HttpConfig(hostUrl(), "v1", null, 5);
        HttpPublisher pub = new HttpPublisher(config);
        assertDoesNotThrow(pub::close);
    }

    // ── publishSessionComplete (agent "all channels uploaded" signal) ──

    @Test
    void publishSessionComplete_postsToEndpointWithSessionHeader_andReturnsTrueOn200() {
        statusToReturn.set(200);
        HttpConfig config = new HttpConfig(hostUrl(), "v1", null, 5);
        HttpPublisher pub = new HttpPublisher(config);

        boolean ok = pub.publishSessionComplete("session-xyz");

        assertTrue(ok);
        assertEquals("/api/v1/events/session-complete", lastPath.get());
        assertEquals("session-xyz", lastSessionId.get());
    }

    @Test
    void publishSessionComplete_oldBackend404_returnsTrueSoAgentDoesNotRetry() {
        // An older backend without the endpoint 404s; the agent must treat it as
        // terminal (the grace path still finalizes the session) and not loop.
        statusToReturn.set(404);
        HttpConfig config = new HttpConfig(hostUrl(), "v1", null, 5);
        boolean ok = new HttpPublisher(config).publishSessionComplete("session-xyz");
        assertTrue(ok);
    }

    @Test
    void publishSessionComplete_serverError500_returnsFalseForRetry() {
        statusToReturn.set(500);
        HttpConfig config = new HttpConfig(hostUrl(), "v1", null, 5);
        boolean ok = new HttpPublisher(config).publishSessionComplete("session-xyz");
        assertFalse(ok);
    }
}
