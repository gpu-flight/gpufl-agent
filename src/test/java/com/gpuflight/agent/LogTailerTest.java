package com.gpuflight.agent;

import com.gpuflight.agent.config.StreamUploadSettings;
import com.gpuflight.agent.model.LogWrapper;
import com.gpuflight.agent.publisher.Publisher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Window-model tailing: the client publishes each rotation as a COMPLETE,
 * immutable file {@code <channel>.<index>.log[.gz]} (index 1 = oldest, higher =
 * newer). The tailer sends each finished window WHOLE, in increasing index order,
 * and finishes once the session's {@code .tmp/} dir is gone and no further window
 * has appeared. There is no live {@code <channel>.log} to tail.
 */
class LogTailerTest {

    static class CapturingPublisher implements Publisher {
        final List<LogWrapper> events = new CopyOnWriteArrayList<>();
        @Override public boolean publish(String topic, String key, LogWrapper log) { events.add(log); return true; }
        @Override public void close() {}
    }

    static class CapturingStreamPublisher implements Publisher {
        final List<List<String>> batches = new CopyOnWriteArrayList<>();
        final List<String> sessionIds = new CopyOnWriteArrayList<>();
        @Override public boolean publish(String topic, String key, LogWrapper log) { return false; }
        @Override public boolean publishStream(String sessionId, List<String> ndjsonLines) {
            sessionIds.add(sessionId); batches.add(List.copyOf(ndjsonLines)); return true;
        }
        @Override public boolean publishStreamGz(String sessionId, byte[] gzBody) {
            // Mirror the backend: decompress the verbatim window gz and record its lines.
            sessionIds.add(sessionId);
            try (var in = new java.util.zip.GZIPInputStream(new java.io.ByteArrayInputStream(gzBody))) {
                String text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
                batches.add(java.util.Arrays.stream(text.split("\n")).filter(s -> !s.isEmpty()).toList());
            } catch (Exception e) { return false; }
            return true;
        }
        @Override public void close() {}
    }

    private static void gzip(Path src, Path dest) throws IOException {
        try (var in = Files.newInputStream(src); var out = new GZIPOutputStream(Files.newOutputStream(dest))) {
            in.transferTo(out);
        }
    }

    /** Publish a complete window {@code <session>/<channel>.<index>.log}. */
    private static Path window(Path dir, String session, String channel, int index, String content) throws IOException {
        Files.createDirectories(dir.resolve(session));
        Path p = dir.resolve(session + "/" + channel + "." + index + ".log");
        Files.writeString(p, content);
        return p;
    }

    /** Publish a compressed window {@code <session>/<channel>.<index>.log.gz}. */
    private static Path gzWindow(Path dir, String session, String channel, int index, String content) throws IOException {
        Path plain = window(dir, session, channel, index, content);
        Path gz = dir.resolve(session + "/" + channel + "." + index + ".log.gz");
        gzip(plain, gz);
        Files.delete(plain);
        return gz;
    }

    private static Thread startTailer(LogTailer tailer, Publisher publisher) {
        Thread t = new Thread(() -> tailer.tail(publisher));
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static void awaitEvents(List<?> list, int count, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (list.size() < count && System.currentTimeMillis() < deadline) Thread.sleep(50);
    }

    private static LogTailer tailer(Path dir, String session, String channel, CursorManager cur) {
        return new LogTailer(dir.toFile(), session, channel, "gpu-trace", cur, new LinkedBlockingQueue<>());
    }

    private static LogTailer streamTailer(Path dir, String session, String channel, CursorManager cur) {
        return new LogTailer(dir.toFile(), session, channel, "gpu-trace", cur, new LinkedBlockingQueue<>(),
                null, new StreamUploadSettings(true, 10, 1_000_000L));
    }

    // ---- a complete window is sent whole ----

    @Test
    void window_readsAndPublishes(@TempDir Path dir) throws Exception {
        window(dir, "app", "device", 1,
            "{\"type\":\"kernel_event\",\"name\":\"k1\"}\n{\"type\":\"kernel_event\",\"name\":\"k2\"}\n");
        var pub = new CapturingPublisher();
        var t = startTailer(tailer(dir, "app", "device", new CursorManager(dir.resolve("cursor.json").toFile())), pub);
        awaitEvents(pub.events, 2, 3000);
        t.interrupt(); t.join(2000);
        assertEquals(2, pub.events.size());
        assertEquals("kernel_event", pub.events.get(0).type());
    }

    @Test
    void window_sassChannel_publishes(@TempDir Path dir) throws Exception {
        window(dir, "app", "sass", 1, "{\"type\":\"cubin_disassembly\"}\n");
        var pub = new CapturingPublisher();
        var t = startTailer(tailer(dir, "app", "sass", new CursorManager(dir.resolve("cursor.json").toFile())), pub);
        awaitEvents(pub.events, 1, 3000);
        t.interrupt(); t.join(2000);
        assertEquals(1, pub.events.size());
        assertEquals("cubin_disassembly", pub.events.get(0).type());
    }

    @Test
    void window_skipsBlankLines(@TempDir Path dir) throws Exception {
        window(dir, "app", "scope", 1, "{\"type\":\"scope_event\"}\n\n   \n{\"type\":\"scope_event\"}\n");
        var pub = new CapturingPublisher();
        var t = startTailer(tailer(dir, "app", "scope", new CursorManager(dir.resolve("cursor.json").toFile())), pub);
        awaitEvents(pub.events, 2, 3000);
        t.interrupt(); t.join(2000);
        assertEquals(2, pub.events.size());
    }

    @Test
    void window_invalidJsonLine_doesNotCrash(@TempDir Path dir) throws Exception {
        window(dir, "app", "system", 1, "not-json-at-all\n{\"type\":\"system_event\"}\n");
        var pub = new CapturingPublisher();
        var t = startTailer(tailer(dir, "app", "system", new CursorManager(dir.resolve("cursor.json").toFile())), pub);
        awaitEvents(pub.events, 1, 3000);
        t.interrupt(); t.join(2000);
        assertEquals(1, pub.events.size());
        assertEquals("system_event", pub.events.get(0).type());
    }

    @Test
    void window_nullQueue_doesNotThrow(@TempDir Path dir) throws Exception {
        window(dir, "app", "device", 1, "{\"type\":\"kernel_event\"}\n");
        var pub = new CapturingPublisher();
        var t = startTailer(new LogTailer(dir.toFile(), "app", "device", "gpu-trace",
            new CursorManager(dir.resolve("cursor.json").toFile()), null), pub);
        awaitEvents(pub.events, 1, 3000);
        t.interrupt(); t.join(2000);
        assertEquals(1, pub.events.size());
    }

    // ---- multiple windows sent in increasing index order ----

    @Test
    void windows_sentInAscendingOrder(@TempDir Path dir) throws Exception {
        window(dir, "app", "device", 1, "{\"type\":\"kernel_event\",\"name\":\"k1\"}\n");
        window(dir, "app", "device", 2, "{\"type\":\"kernel_event\",\"name\":\"k2\"}\n");
        window(dir, "app", "device", 3, "{\"type\":\"kernel_event\",\"name\":\"k3\"}\n");
        var pub = new CapturingPublisher();
        var t = startTailer(tailer(dir, "app", "device", new CursorManager(dir.resolve("cursor.json").toFile())), pub);
        awaitEvents(pub.events, 3, 4000);
        t.interrupt(); t.join(2000);
        assertEquals(3, pub.events.size());
        assertTrue(pub.events.get(0).data().contains("k1"));
        assertTrue(pub.events.get(1).data().contains("k2"));
        assertTrue(pub.events.get(2).data().contains("k3"));
    }

    // ---- gz windows + mid-window resume ----

    @Test
    void gzWindow_reads(@TempDir Path dir) throws Exception {
        gzWindow(dir, "app", "device", 1,
            "{\"type\":\"kernel_event\",\"name\":\"k1\"}\n{\"type\":\"kernel_event\",\"name\":\"k2\"}\n");
        var pub = new CapturingPublisher();
        var t = startTailer(tailer(dir, "app", "device", new CursorManager(dir.resolve("cursor.json").toFile())), pub);
        awaitEvents(pub.events, 2, 3000);
        t.interrupt(); t.join(2000);
        assertEquals(2, pub.events.size());
    }

    @Test
    void gzWindow_resumesFromOffset(@TempDir Path dir) throws Exception {
        String l1 = "{\"type\":\"kernel_event\",\"name\":\"k1\"}\n";
        gzWindow(dir, "app", "device", 1, l1 + "{\"type\":\"kernel_event\",\"name\":\"k2\"}\n");
        long off = l1.getBytes(StandardCharsets.UTF_8).length;
        File cursorFile = dir.resolve("cursor.json").toFile();
        Files.writeString(cursorFile.toPath(), "{\"streams\":{\"app.device\":{\"fileIndex\":1,\"offset\":" + off + "}}}");
        var pub = new CapturingPublisher();
        var t = startTailer(tailer(dir, "app", "device", new CursorManager(cursorFile)), pub);
        awaitEvents(pub.events, 1, 3000);
        t.interrupt(); t.join(2000);
        assertEquals(1, pub.events.size(), "only the tail after the offset should be read");
        assertTrue(pub.events.get(0).data().contains("k2"));
    }

    // ---- cursor advances past a sent window ----

    @Test
    void cursor_advancesAfterWindow(@TempDir Path dir) throws Exception {
        window(dir, "app", "device", 1, "{\"type\":\"kernel_event\"}\n");
        File cursorFile = dir.resolve("cursor.json").toFile();
        var pub = new CapturingPublisher();
        var t = startTailer(tailer(dir, "app", "device", new CursorManager(cursorFile)), pub);
        awaitEvents(pub.events, 1, 3000);
        Thread.sleep(200); // let the loop advance the cursor past window 1
        t.interrupt(); t.join(2000);
        CursorPosition pos = new CursorManager(cursorFile).get("app.device");
        assertTrue(pos.fileIndex() >= 1, "cursor should sit on/past window 1");
    }

    @Test
    void streamMode_publishesBatch(@TempDir Path dir) throws Exception {
        window(dir, "app", "device", 1,
            "{\"type\":\"kernel_event\",\"session_id\":\"app\",\"name\":\"k1\"}\n" +
            "{\"type\":\"kernel_event\",\"session_id\":\"app\",\"name\":\"k2\"}\n");
        var pub = new CapturingStreamPublisher();
        var t = startTailer(streamTailer(dir, "app", "device", new CursorManager(dir.resolve("cursor.json").toFile())), pub);
        awaitEvents(pub.batches, 1, 3000);
        t.interrupt(); t.join(2000);
        assertEquals(List.of("app"), pub.sessionIds);
        assertEquals(2, pub.batches.get(0).size());
        assertTrue(pub.batches.get(0).get(0).contains("\"k1\""));
    }

    @Test
    void streamMode_gzWindow_sentAsOneChunk(@TempDir Path dir) throws Exception {
        // A .gz window is shipped verbatim as ONE chunk (no decompress/re-batch),
        // and the cursor advances past it so a crash/restart never re-sends it.
        gzWindow(dir, "app", "device", 1,
            "{\"type\":\"kernel_event\",\"name\":\"k1\"}\n{\"type\":\"kernel_event\",\"name\":\"k2\"}\n");
        File cursorFile = dir.resolve("cursor.json").toFile();
        var pub = new CapturingStreamPublisher();
        var t = startTailer(streamTailer(dir, "app", "device", new CursorManager(cursorFile)), pub);
        awaitEvents(pub.batches, 1, 3000);
        Thread.sleep(200); // let the window-done advance persist
        t.interrupt(); t.join(2000);
        assertEquals(1, pub.batches.size(), "the whole gz window is sent as ONE chunk");
        assertEquals(2, pub.batches.get(0).size());
        assertTrue(pub.batches.get(0).get(0).contains("k1"));
        assertTrue(pub.batches.get(0).get(1).contains("k2"));
        assertTrue(new CursorManager(cursorFile).get("app.device").fileIndex() >= 2,
            "cursor must advance past the sent window so a restart does not re-send it");
    }

    // ---- waits while the session is still writing (.tmp/ present) ----

    @Test
    void waitsWhileTmpPresent_thenSendsLateWindow(@TempDir Path dir) throws Exception {
        Files.createDirectories(dir.resolve("app/.tmp")); // session writing, no window yet
        var pub = new CapturingPublisher();
        var t = startTailer(tailer(dir, "app", "device", new CursorManager(dir.resolve("cursor.json").toFile())), pub);
        Thread.sleep(300);
        assertTrue(pub.events.isEmpty(), "no window published yet -> nothing sent");
        window(dir, "app", "device", 1, "{\"type\":\"kernel_event\"}\n");
        awaitEvents(pub.events, 1, 5000);
        t.interrupt(); t.join(2000);
        assertEquals(1, pub.events.size());
    }

    // ---- a finished session (no .tmp/) exits on its own and is not re-uploaded ----

    @Test
    void finishedSession_exitsAndNotReuploaded(@TempDir Path dir) throws Exception {
        gzWindow(dir, "app", "device", 1,
            "{\"type\":\"kernel_event\",\"session_id\":\"app\",\"name\":\"k1\"}\n" +
            "{\"type\":\"kernel_event\",\"session_id\":\"app\",\"name\":\"k2\"}\n");
        File cursorFile = dir.resolve("cursor.json").toFile();

        // Run 1: no .tmp/ -> send the one window, then exit on its own (after the finish grace).
        var pub1 = new CapturingStreamPublisher();
        var t1 = startTailer(streamTailer(dir, "app", "device", new CursorManager(cursorFile)), pub1);
        t1.join(8000);
        assertFalse(t1.isAlive(), "tailer should exit on its own once the finished session is drained");
        assertEquals(1, pub1.batches.size(), "run 1 sends the finished session once");

        // Run 2: same files + persisted cursor -> nothing re-sent.
        var pub2 = new CapturingStreamPublisher();
        var t2 = startTailer(streamTailer(dir, "app", "device", new CursorManager(cursorFile)), pub2);
        t2.join(8000);
        assertFalse(t2.isAlive(), "restart of a finished session should exit");
        assertTrue(pub2.batches.isEmpty(), "a finished session must not be re-uploaded on restart");
    }

    // ---- the sent window is offered to the archive queue ----

    @Test
    void sentWindow_offeredToQueue(@TempDir Path dir) throws Exception {
        Path gz = gzWindow(dir, "app", "device", 1, "{\"type\":\"kernel_event\",\"name\":\"k1\"}\n");
        LinkedBlockingQueue<Path> queue = new LinkedBlockingQueue<>();
        var pub = new CapturingPublisher();
        var t = startTailer(new LogTailer(dir.toFile(), "app", "device", "gpu-trace",
            new CursorManager(dir.resolve("cursor.json").toFile()), queue), pub);
        Path consumed = queue.poll(3, TimeUnit.SECONDS);
        t.interrupt(); t.join(2000);
        assertNotNull(consumed, "the sent window should be offered to the archive queue");
        assertEquals(gz.toAbsolutePath(), consumed.toAbsolutePath());
    }
}
