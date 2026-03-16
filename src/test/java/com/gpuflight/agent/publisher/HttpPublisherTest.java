package com.gpuflight.agent.publisher;

import com.gpuflight.agent.config.HttpConfig;
import com.gpuflight.agent.model.LogWrapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class HttpPublisherTest {

    private HttpServer server;
    private int port;
    private final AtomicInteger statusToReturn = new AtomicInteger(200);
    private final AtomicReference<String> lastBody = new AtomicReference<>();
    private final AtomicReference<String> lastAuthHeader = new AtomicReference<>();

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/", exchange -> {
            lastBody.set(new String(exchange.getRequestBody().readAllBytes()));
            lastAuthHeader.set(exchange.getRequestHeaders().getFirst("Authorization"));
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

    private String baseUrl() {
        return "http://localhost:" + port + "/api/";
    }

    private LogWrapper sampleLog(String type) {
        return new LogWrapper(System.currentTimeMillis(), "{\"key\":\"val\"}", type, "testhost", "127.0.0.1");
    }

    @Test
    void publish_sendsPostRequest() throws InterruptedException {
        HttpConfig config = new HttpConfig(baseUrl(), null, 5);
        HttpPublisher pub = new HttpPublisher(config);

        pub.publish("topic", "device", sampleLog("kernel_event"));

        // Give the request time to arrive
        Thread.sleep(300);
        assertNotNull(lastBody.get(), "Server should have received a request body");
        assertTrue(lastBody.get().contains("kernel_event"));
    }

    @Test
    void publish_sendsAuthorizationHeader() throws InterruptedException {
        HttpConfig config = new HttpConfig(baseUrl(), "gpfl_tok123", 5);
        HttpPublisher pub = new HttpPublisher(config);

        pub.publish("topic", "scope", sampleLog("scope_event"));
        Thread.sleep(300);

        assertEquals("Bearer gpfl_tok123", lastAuthHeader.get());
    }

    @Test
    void publish_noToken_doesNotSendAuthHeader() throws InterruptedException {
        HttpConfig config = new HttpConfig(baseUrl(), null, 5);
        HttpPublisher pub = new HttpPublisher(config);

        pub.publish("topic", "system", sampleLog("system_event"));
        Thread.sleep(300);

        assertNull(lastAuthHeader.get());
    }

    @Test
    void publish_nonSuccessResponse_doesNotThrow() throws InterruptedException {
        statusToReturn.set(500);
        HttpConfig config = new HttpConfig(baseUrl(), null, 5);
        HttpPublisher pub = new HttpPublisher(config);

        // Should log the error but not throw
        assertDoesNotThrow(() -> pub.publish("topic", "device", sampleLog("kernel_event")));
        Thread.sleep(300);
    }

    @Test
    void publish_unreachableServer_doesNotThrow() {
        HttpConfig config = new HttpConfig("http://localhost:1/api/", null, 1);
        HttpPublisher pub = new HttpPublisher(config);

        // Should swallow the connection error
        assertDoesNotThrow(() -> pub.publish("topic", "device", sampleLog("kernel_event")));
    }

    @Test
    void close_doesNotThrow() {
        HttpConfig config = new HttpConfig(baseUrl(), null, 5);
        HttpPublisher pub = new HttpPublisher(config);
        assertDoesNotThrow(pub::close);
    }
}
