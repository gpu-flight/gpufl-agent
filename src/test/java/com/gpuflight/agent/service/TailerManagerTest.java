package com.gpuflight.agent.service;

import com.gpuflight.agent.CursorManager;
import com.gpuflight.agent.SessionOwnership;
import com.gpuflight.agent.config.ConfigLoader;
import com.gpuflight.agent.config.HttpConfig;
import com.gpuflight.agent.config.JsonSettings;
import com.gpuflight.agent.config.StreamUploadSettings;
import com.gpuflight.agent.model.DiscoveredSession;
import com.gpuflight.agent.model.LogWrapper;
import com.gpuflight.agent.model.WindowMetadata;
import com.gpuflight.agent.publisher.HttpPublisher;
import com.gpuflight.agent.publisher.Publisher;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Once a session's tailers finish, the manager "settles" it so a re-scan skips it:
 * a producer that closed cleanly (.tmp/ removed) → {@code .uploaded}; one left with a
 * stale/orphaned .tmp/ (producer crashed) → {@code .failed}, or deleted when pruning
 * is enabled (its windows are already uploaded).
 */
class TailerManagerTest {

    /** Accepts everything via the legacy path; stream/session-complete use interface defaults. */
    static final class NoopPublisher implements Publisher {
        @Override public boolean publish(String topic, String key, LogWrapper log) { return true; }
        @Override public void close() {}
    }

    static final class CompletionPublisher implements Publisher {
        volatile int completionSignals;
        @Override public boolean publish(String topic, String key, LogWrapper log) {
            return true;
        }
        @Override public boolean publishStream(
                String sessionId, List<String> ndjsonLines) {
            return true;
        }
        @Override public boolean publishSessionComplete(String sessionId) {
            completionSignals++;
            return true;
        }
        @Override public void close() {}
    }

    private TailerManager managerIn(ExecutorService ex, Path root) {
        return new TailerManager(ex, new NoopPublisher(),
                new CursorManager(new File(root.toFile(), "cursor.json")),
                new LinkedBlockingQueue<>(), null, StreamUploadSettings.DISABLED, "gpu-trace");
    }

    private TailerManager streamManagerIn(
            ExecutorService ex, Path root, Publisher publisher) {
        return new TailerManager(ex, publisher,
                new CursorManager(new File(root.toFile(), "cursor.json")),
                new LinkedBlockingQueue<>(), null,
                new StreamUploadSettings(true, 100, 1_000_000L),
                "gpu-trace");
    }

    private static void runToCompletion(ExecutorService ex) throws InterruptedException {
        ex.shutdown();
        assertTrue(ex.awaitTermination(20, TimeUnit.SECONDS), "tailers did not finish");
    }

    @Test
    void cleanFinishMarksUploaded(@TempDir Path root) throws Exception {
        File sess = new File(root.toFile(), "sess-clean");
        Files.createDirectories(sess.toPath());
        Files.writeString(new File(sess, "device.1.log").toPath(), "x\n"); // final window, no .tmp/

        ExecutorService ex = Executors.newVirtualThreadPerTaskExecutor();
        managerIn(ex, root).spawnSessionTailers(
                new DiscoveredSession(root.toFile(), "sess-clean", List.of("device")));
        runToCompletion(ex);

        assertTrue(new File(sess, SessionWatcher.UPLOADED_MARKER).exists());
        assertFalse(new File(sess, SessionWatcher.FAILED_MARKER).exists());
    }

    @Test
    void orphanedFinishMarksFailed(@TempDir Path root) throws Exception {
        File sess = new File(root.toFile(), "sess-orphan");
        Files.createDirectories(new File(sess, ".tmp").toPath());
        Files.writeString(new File(sess, "device.1.log").toPath(), "x\n");
        new File(sess, ".tmp").setLastModified(System.currentTimeMillis() - 120_000L); // orphaned

        ExecutorService ex = Executors.newVirtualThreadPerTaskExecutor();
        managerIn(ex, root).spawnSessionTailers(
                new DiscoveredSession(root.toFile(), "sess-orphan", List.of("device")));
        runToCompletion(ex);

        assertTrue(new File(sess, SessionWatcher.FAILED_MARKER).exists());
        assertFalse(new File(sess, SessionWatcher.UPLOADED_MARKER).exists());
    }

    @Test
    void orphanedFinishPrunesWhenEnabled(@TempDir Path root) throws Exception {
        File sess = new File(root.toFile(), "sess-prune");
        Files.createDirectories(new File(sess, ".tmp").toPath());
        Files.writeString(new File(sess, "device.1.log").toPath(), "x\n");
        new File(sess, ".tmp").setLastModified(System.currentTimeMillis() - 120_000L);

        ExecutorService ex = Executors.newVirtualThreadPerTaskExecutor();
        TailerManager m = managerIn(ex, root);
        m.setPruneFailed(true);
        m.spawnSessionTailers(new DiscoveredSession(root.toFile(), "sess-prune", List.of("device")));
        runToCompletion(ex);

        assertFalse(sess.exists(), "orphaned session should be pruned");
    }

    @Test
    void transportLossNeverSignalsSessionComplete(@TempDir Path root)
            throws Exception {
        File sess = new File(root.toFile(), "sess-loss");
        Files.createDirectories(sess.toPath());
        Files.writeString(new File(sess, "device.1.log").toPath(),
                "{\"type\":\"shutdown\",\"session_id\":\"sess-loss\"}\n");
        Files.writeString(
                new File(sess,
                    ".gpufl-transport-loss.device.1.json").toPath(), "{}");

        CompletionPublisher publisher = new CompletionPublisher();
        ExecutorService ex = Executors.newVirtualThreadPerTaskExecutor();
        streamManagerIn(ex, root, publisher).spawnSessionTailers(
                new DiscoveredSession(
                    root.toFile(), "sess-loss", List.of("device")));
        runToCompletion(ex);

        assertEquals(0, publisher.completionSignals);
        assertTrue(new File(sess, SessionWatcher.FAILED_MARKER).exists());
        assertFalse(new File(sess, SessionWatcher.UPLOADED_MARKER).exists());
    }

    @Test
    void servingOnlySessionUploadsAndCompletesOnceItsLockIsReleased(
            @TempDir Path root) throws Exception {
        // The Python serving writer's layout: serving windows only, no native channel.
        Path sess = Files.createDirectories(root.resolve("serving-sess"));
        Files.createDirectories(sess.resolve(".tmp"));
        byte[] gz = gzip("{\"type\":\"serving_request\",\"session_id\":\"serving-sess\"}\n");
        Files.write(sess.resolve("serving.1.log.gz"), gz);
        CRC32 crc = new CRC32();
        crc.update(gz);
        Files.writeString(sess.resolve(".gpufl-window.serving.1.json"),
                JsonSettings.MAPPER.writeValueAsString(new WindowMetadata(
                        1, "transport_window", "11111111-2222-4333-8444-555555555555",
                        "serving-sess", "serving", 1, 10, 20, 30,
                        "serving.1.log.gz", gz.length, crc.getValue())));
        Path lock = Files.writeString(sess.resolve(SessionOwnership.LOCK_FILE), "");

        // Fake backend: acknowledges the window identity and the completion signal.
        List<String> channels = new CopyOnWriteArrayList<>();
        AtomicInteger completions = new AtomicInteger();
        CountDownLatch uploaded = new CountDownLatch(1);
        HttpServer backend = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        backend.createContext("/api/v1/events/", exchange -> {
            exchange.getRequestBody().readAllBytes();
            String path = exchange.getRequestURI().getPath();
            String body = "{}";
            if (path.endsWith("/stream")) {
                channels.add(exchange.getRequestHeaders().getFirst("X-GpuFlight-Window-Channel"));
                body = "{\"window_acknowledged\":true}";
                uploaded.countDown();
            } else if (path.endsWith("/session-complete")) {
                completions.incrementAndGet();
            }
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        backend.start();
        try {
            HttpConfig http = new HttpConfig(
                    "http://127.0.0.1:" + backend.getAddress().getPort(), "v1", null, 5);
            ExecutorService ex = Executors.newVirtualThreadPerTaskExecutor();
            TailerManager manager = new TailerManager(ex, new HttpPublisher(http),
                    new CursorManager(new File(root.toFile(), "cursor.json")),
                    new LinkedBlockingQueue<>(), null, StreamUploadSettings.from(http),
                    "gpu-trace");

            try (FileChannel writer = FileChannel.open(
                    lock, StandardOpenOption.READ, StandardOpenOption.WRITE);
                 FileLock ignored = writer.lock()) {
                List<DiscoveredSession> found = SessionWatcher.discoverSources(
                        root.toFile(), ConfigLoader.DEFAULT_LOG_TYPES);
                assertEquals(List.of("serving-sess"),
                        found.stream().map(DiscoveredSession::sessionId).toList());
                manager.spawnSessionTailers(found.get(0));

                // A finished window uploads while the writer still holds the lock...
                assertTrue(uploaded.await(10, TimeUnit.SECONDS), "serving window not uploaded");
                assertEquals(List.of("serving"), channels);
                assertEquals(0, completions.get());
                Files.delete(sess.resolve(".tmp"));
            }
            // ...and the session completes once the writer releases it.
            runToCompletion(ex);
            assertEquals(1, completions.get());
            assertTrue(new File(sess.toFile(), SessionWatcher.UPLOADED_MARKER).exists());
        } finally {
            backend.stop(0);
        }
    }

    private static byte[] gzip(String text) throws IOException {
        var out = new ByteArrayOutputStream();
        try (var gz = new GZIPOutputStream(out)) {
            gz.write(text.getBytes(StandardCharsets.UTF_8));
        }
        return out.toByteArray();
    }
}
