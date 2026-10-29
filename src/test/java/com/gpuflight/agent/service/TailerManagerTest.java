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
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
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
        publishWindow(sess, "serving", 1);
        Path lock = Files.writeString(sess.resolve(SessionOwnership.LOCK_FILE), "");

        try (FakeBackend backend = new FakeBackend()) {
            ExecutorService ex = Executors.newVirtualThreadPerTaskExecutor();
            TailerManager manager = backend.managerIn(ex, root);

            try (FileChannel writer = FileChannel.open(
                    lock, StandardOpenOption.READ, StandardOpenOption.WRITE);
                 FileLock ignored = writer.lock()) {
                // No configuration: the serving channel is found, not listed.
                manager.spawnSessionTailers(discoverOnly(root));

                // A finished window uploads while the writer still holds the lock...
                backend.awaitEvent("serving");
                assertEquals(List.of("serving"), backend.events);
                Files.delete(sess.resolve(".tmp"));
            }
            // ...and the session completes once the writer releases it.
            runToCompletion(ex);
            assertEquals(List.of("serving", COMPLETE), backend.events);
            assertTrue(new File(sess.toFile(), SessionWatcher.UPLOADED_MARKER).exists());
        }
    }

    @Test
    void bareApplicationLogIsNeverUploadedOrTakenForAChannel(@TempDir Path root)
            throws Exception {
        // An application log next to the windows can hold prompts. It has no sidecar,
        // so it is no channel, and a directory holding only such logs is no session.
        String prompt = "{\"type\":\"request\",\"prompt\":\"do not upload\"}\n";
        Path sess = Files.createDirectories(root.resolve("serving-sess"));
        publishWindow(sess, "serving", 1);
        Files.writeString(sess.resolve("server.log"), prompt);
        Files.writeString(sess.resolve("server.1.log"), prompt);
        Path appOnly = Files.createDirectories(root.resolve("app-only"));
        Files.writeString(appOnly.resolve("server.log"), prompt);

        try (FakeBackend backend = new FakeBackend()) {
            ExecutorService ex = Executors.newVirtualThreadPerTaskExecutor();
            TailerManager manager = backend.managerIn(ex, root);
            DiscoveredSession found = discoverOnly(root);
            assertEquals("serving-sess", found.sessionId());
            assertEquals(List.of("serving"), found.logTypes());
            manager.spawnSessionTailers(found);
            runToCompletion(ex);

            assertEquals(List.of("serving", COMPLETE), backend.events);
            assertFalse(new File(appOnly.toFile(), SessionWatcher.UPLOADED_MARKER).exists());
        }
    }

    @Test
    void nativeSessionUploadsExactlyItsFourChannels(@TempDir Path root) throws Exception {
        Path sess = Files.createDirectories(root.resolve("native-sess"));
        for (String channel : List.of("device", "scope", "system", "sass")) {
            publishWindow(sess, channel, 1);
        }

        try (FakeBackend backend = new FakeBackend()) {
            uploadWithNoConfiguration(backend, root);
            assertEquals(List.of("device", "sass", "scope", "system"), backend.windows());
            assertEquals(1, backend.completions());
        }
    }

    @Test
    void unknownProducerChannelUploadsWithNoConfiguration(@TempDir Path root)
            throws Exception {
        Path sess = Files.createDirectories(root.resolve("foo-sess"));
        publishWindow(sess, "foo", 1);
        publishWindow(sess, "foo", 2);

        try (FakeBackend backend = new FakeBackend()) {
            uploadWithNoConfiguration(backend, root);
            assertEquals(List.of("foo", "foo", COMPLETE), backend.events);
        }
    }

    @Test
    void channelsPublishedLateAreUploadedBeforeCompletion(@TempDir Path root)
            throws Exception {
        Path sess = Files.createDirectories(root.resolve("late-sess"));
        Files.createDirectories(sess.resolve(".tmp"));
        publishWindow(sess, "serving", 1);
        Path lock = Files.writeString(sess.resolve(SessionOwnership.LOCK_FILE), "");

        try (FakeBackend backend = new FakeBackend()) {
            ExecutorService ex = Executors.newVirtualThreadPerTaskExecutor();
            TailerManager manager = backend.managerIn(ex, root);
            try (FileChannel writer = FileChannel.open(
                    lock, StandardOpenOption.READ, StandardOpenOption.WRITE);
                 FileLock ignored = writer.lock()) {
                assertTrue(manager.spawnSessionTailers(discoverOnly(root)));
                backend.awaitEvent("serving");

                // A channel's first window published while the session is live: the
                // next scan gives it a tailer.
                publishWindow(sess, "foo", 1);
                assertTrue(manager.spawnSessionTailers(discoverOnly(root)));
                backend.awaitEvent("foo");
                assertFalse(manager.spawnSessionTailers(discoverOnly(root)));

                // One published just before the producer finishes, with no scan after it.
                publishWindow(sess, "bar", 1);
                Files.delete(sess.resolve(".tmp"));
            }
            awaitUploaded(sess);
            runToCompletion(ex);
            assertEquals(List.of("serving", "foo", "bar", COMPLETE), backend.events);
        }
    }

    @Test
    void sessionOverTheChannelCapIsNeverReportedComplete(@TempDir Path root)
            throws Exception {
        Path sess = Files.createDirectories(root.resolve("broken-sess"));
        for (int i = 0; i <= SessionWatcher.MAX_CHANNELS_PER_SESSION; i++) {
            publishWindow(sess, "ch" + (char) ('a' + i), 1);
        }

        try (FakeBackend backend = new FakeBackend()) {
            uploadWithNoConfiguration(backend, root);
            assertEquals(SessionWatcher.MAX_CHANNELS_PER_SESSION, backend.windows().size());
            assertEquals(0, backend.completions());
            assertTrue(new File(sess.toFile(), SessionWatcher.FAILED_MARKER).exists());
            assertTrue(Files.exists(sess.resolve("chq.1.log.gz")), "unsent window was removed");
        }
    }

    @Test
    void agentNotTailingServingNeverAdoptsServingOnlySession(@TempDir Path root)
            throws Exception {
        Path sess = Files.createDirectories(root.resolve("serving-sess"));
        publishWindow(sess, "serving", 1);

        CompletionPublisher publisher = new CompletionPublisher();
        ExecutorService ex = Executors.newVirtualThreadPerTaskExecutor();
        TailerManager manager = streamManagerIn(ex, root, publisher);
        // The launcher's log types today: native channels only.
        for (DiscoveredSession s : SessionWatcher.discoverSources(
                root.toFile(), List.of("device", "scope", "system", "sass"))) {
            manager.spawnSessionTailers(s);
        }
        runToCompletion(ex);

        assertFalse(manager.hasStartedAnySession());
        assertEquals(0, publisher.completionSignals);
        assertFalse(new File(sess.toFile(), SessionWatcher.UPLOADED_MARKER).exists());
    }

    private static final String COMPLETE = "session-complete";

    /** Fake backend: acknowledges every window identity and records, in arrival order,
     *  each window's channel and each session-complete signal. */
    private static final class FakeBackend implements AutoCloseable {
        final List<String> events = new CopyOnWriteArrayList<>();
        private final HttpServer server;

        FakeBackend() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/api/v1/events/", exchange -> {
                exchange.getRequestBody().readAllBytes();
                String path = exchange.getRequestURI().getPath();
                String body = "{}";
                if (path.endsWith("/stream")) {
                    String channel = exchange.getRequestHeaders()
                            .getFirst("X-GpuFlight-Window-Channel");
                    events.add(channel == null ? "batch-without-window-identity" : channel);
                    body = "{\"window_acknowledged\":true}";
                } else if (path.endsWith("/session-complete")) {
                    events.add(COMPLETE);
                }
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, bytes.length);
                exchange.getResponseBody().write(bytes);
                exchange.close();
            });
            server.start();
        }

        TailerManager managerIn(ExecutorService ex, Path root) {
            HttpConfig http = new HttpConfig(
                    "http://127.0.0.1:" + server.getAddress().getPort(), "v1", null, 5);
            return new TailerManager(ex, new HttpPublisher(http),
                    new CursorManager(new File(root.toFile(), "cursor.json")),
                    new LinkedBlockingQueue<>(), null, StreamUploadSettings.from(http),
                    "gpu-trace");
        }

        List<String> windows() {
            return events.stream().filter(e -> !COMPLETE.equals(e)).sorted().toList();
        }

        long completions() {
            return events.stream().filter(COMPLETE::equals).count();
        }

        void awaitEvent(String event) throws InterruptedException {
            for (int i = 0; i < 100 && !events.contains(event); i++) Thread.sleep(100);
            assertTrue(events.contains(event), event + " not received");
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }

    // A producer's window: its sidecar first, then the gzip payload the sidecar describes.
    private static void publishWindow(Path sess, String channel, int sequence)
            throws IOException {
        String sid = sess.getFileName().toString();
        byte[] gz = gzip("{\"type\":\"" + channel + "_record\",\"session_id\":\"" + sid + "\"}\n");
        CRC32 crc = new CRC32();
        crc.update(gz);
        String payload = channel + "." + sequence + ".log.gz";
        Files.writeString(sess.resolve(".gpufl-window." + channel + "." + sequence + ".json"),
                JsonSettings.MAPPER.writeValueAsString(new WindowMetadata(
                        1, "transport_window", UUID.randomUUID().toString(), sid, channel,
                        sequence, 10, 20, 30, payload, gz.length, crc.getValue())));
        Files.write(sess.resolve(payload), gz);
    }

    private static DiscoveredSession discoverOnly(Path root) {
        List<DiscoveredSession> found = SessionWatcher.discoverSources(
                root.toFile(), ConfigLoader.DEFAULT_LOG_TYPES);
        assertEquals(1, found.size());
        return found.get(0);
    }

    // Discovers and tails the one session under root as an agent with no --log-types.
    private static void uploadWithNoConfiguration(FakeBackend backend, Path root)
            throws InterruptedException {
        ExecutorService ex = Executors.newVirtualThreadPerTaskExecutor();
        backend.managerIn(ex, root).spawnSessionTailers(discoverOnly(root));
        runToCompletion(ex);
    }

    // Waits with the executor still open, since completing may start a late tailer.
    private static void awaitUploaded(Path sess) throws InterruptedException {
        Path marker = sess.resolve(SessionWatcher.UPLOADED_MARKER);
        for (int i = 0; i < 300 && !Files.exists(marker); i++) Thread.sleep(100);
        assertTrue(Files.exists(marker), "session not settled");
    }

    private static byte[] gzip(String text) throws IOException {
        var out = new ByteArrayOutputStream();
        try (var gz = new GZIPOutputStream(out)) {
            gz.write(text.getBytes(StandardCharsets.UTF_8));
        }
        return out.toByteArray();
    }
}
