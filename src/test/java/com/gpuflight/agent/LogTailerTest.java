package com.gpuflight.agent;

import com.gpuflight.agent.model.LogWrapper;
import com.gpuflight.agent.publisher.Publisher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;

class LogTailerTest {

    /** Simple in-memory publisher for testing. */
    static class CapturingPublisher implements Publisher {
        final List<LogWrapper> events = new CopyOnWriteArrayList<>();

        @Override
        public boolean publish(String topic, String key, LogWrapper log) {
            events.add(log);
            return true;
        }

        @Override
        public void close() {}
    }

    /** Publisher that rejects any line whose data contains a marker (simulates
     *  a backend that won't accept a specific event), accepting everything else. */
    static class RejectingPublisher implements Publisher {
        final List<LogWrapper> events = new CopyOnWriteArrayList<>();
        final String reject;
        RejectingPublisher(String reject) { this.reject = reject; }

        @Override
        public boolean publish(String topic, String key, LogWrapper log) {
            if (reject != null && log.data().contains(reject)) return false;
            events.add(log);
            return true;
        }

        @Override
        public void close() {}
    }

    /** gzip src → dest (used to simulate the C++ client compressing a rotated file). */
    private static void gzip(Path src, Path dest) throws IOException {
        try (var in = Files.newInputStream(src);
             var out = new GZIPOutputStream(Files.newOutputStream(dest))) {
            in.transferTo(out);
        }
    }

    private static Thread startTailer(LogTailer tailer, Publisher publisher) {
        Thread t = new Thread(() -> tailer.tail(publisher));
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static void awaitEvents(List<?> list, int count, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (list.size() < count && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
    }

    // ---- happy-path: reads existing lines ----

    @Test
    void tail_readsExistingLinesAndPublishes(@TempDir Path tempDir) throws Exception {
        Path logFile = tempDir.resolve("app.device.log");
        Files.writeString(logFile,
            "{\"type\":\"kernel_event\",\"name\":\"k1\"}\n" +
            "{\"type\":\"kernel_event\",\"name\":\"k2\"}\n"
        );

        CursorManager cursorMgr = new CursorManager(tempDir.resolve("cursor.json").toFile());
        CapturingPublisher publisher = new CapturingPublisher();
        LogTailer tailer = new LogTailer(tempDir.toFile(), "app", "device", "gpu-trace", cursorMgr, new LinkedBlockingQueue<>());

        Thread t = startTailer(tailer, publisher);
        awaitEvents(publisher.events, 2, 3000);
        t.interrupt();
        t.join(2000);

        assertEquals(2, publisher.events.size());
        assertEquals("kernel_event", publisher.events.get(0).type());
        assertEquals("kernel_event", publisher.events.get(1).type());
    }

    @Test
    void tail_skipsBlankLines(@TempDir Path tempDir) throws Exception {
        Path logFile = tempDir.resolve("app.scope.log");
        Files.writeString(logFile,
            "{\"type\":\"scope_event\"}\n" +
            "\n" +
            "   \n" +
            "{\"type\":\"scope_event\"}\n"
        );

        CursorManager cursorMgr = new CursorManager(tempDir.resolve("cursor.json").toFile());
        CapturingPublisher publisher = new CapturingPublisher();
        LogTailer tailer = new LogTailer(tempDir.toFile(), "app", "scope", "gpu-trace", cursorMgr, new LinkedBlockingQueue<>());

        Thread t = startTailer(tailer, publisher);
        awaitEvents(publisher.events, 2, 3000);
        t.interrupt();
        t.join(2000);

        assertEquals(2, publisher.events.size());
    }

    @Test
    void tail_invalidJsonLine_doesNotCrash(@TempDir Path tempDir) throws Exception {
        Path logFile = tempDir.resolve("app.system.log");
        Files.writeString(logFile,
            "not-json-at-all\n" +
            "{\"type\":\"system_event\"}\n"
        );

        CursorManager cursorMgr = new CursorManager(tempDir.resolve("cursor.json").toFile());
        CapturingPublisher publisher = new CapturingPublisher();
        LogTailer tailer = new LogTailer(tempDir.toFile(), "app", "system", "gpu-trace", cursorMgr, new LinkedBlockingQueue<>());

        Thread t = startTailer(tailer, publisher);
        awaitEvents(publisher.events, 1, 3000);
        t.interrupt();
        t.join(2000);

        // Only the valid JSON line is published
        assertEquals(1, publisher.events.size());
        assertEquals("system_event", publisher.events.get(0).type());
    }

    // ---- cursor persistence ----

    @Test
    void tail_persistsCursorAfterReading(@TempDir Path tempDir) throws Exception {
        Path logFile = tempDir.resolve("app.device.log");
        Files.writeString(logFile, "{\"type\":\"kernel_event\"}\n");

        File cursorFile = tempDir.resolve("cursor.json").toFile();
        CursorManager cursorMgr = new CursorManager(cursorFile);
        CapturingPublisher publisher = new CapturingPublisher();
        LogTailer tailer = new LogTailer(tempDir.toFile(), "app", "device", "gpu-trace", cursorMgr, new LinkedBlockingQueue<>());

        Thread t = startTailer(tailer, publisher);
        awaitEvents(publisher.events, 1, 3000);
        t.interrupt();
        t.join(2000);

        // Reload cursor and verify it advanced
        CursorManager mgr2 = new CursorManager(cursorFile);
        CursorPosition pos = mgr2.get("app.device");
        assertEquals(0, pos.fileIndex());
        assertTrue(pos.offset() > 0, "Cursor offset should have advanced past the line");
    }

    // ---- waits when active file is absent ----

    @Test
    void tail_waitsForActiveFileToAppear(@TempDir Path tempDir) throws Exception {
        // No log file exists initially — tailer should wait
        CursorManager cursorMgr = new CursorManager(tempDir.resolve("cursor.json").toFile());
        CapturingPublisher publisher = new CapturingPublisher();
        LogTailer tailer = new LogTailer(tempDir.toFile(), "app", "device", "gpu-trace", cursorMgr, new LinkedBlockingQueue<>());

        Thread t = startTailer(tailer, publisher);
        Thread.sleep(300); // let it enter the waiting branch

        // Now create the file with content
        Path logFile = tempDir.resolve("app.device.log");
        Files.writeString(logFile, "{\"type\":\"kernel_event\"}\n");

        awaitEvents(publisher.events, 1, 5000);
        t.interrupt();
        t.join(2000);

        assertEquals(1, publisher.events.size());
    }

    // ---- null consumedFilesQueue is handled gracefully ----

    @Test
    void tail_nullQueue_doesNotThrow(@TempDir Path tempDir) throws Exception {
        Path logFile = tempDir.resolve("app.device.log");
        Files.writeString(logFile, "{\"type\":\"kernel_event\"}\n");

        CursorManager cursorMgr = new CursorManager(tempDir.resolve("cursor.json").toFile());
        CapturingPublisher publisher = new CapturingPublisher();
        // Pass null for the queue — should not throw
        LogTailer tailer = new LogTailer(tempDir.toFile(), "app", "device", "gpu-trace", cursorMgr, null);

        Thread t = startTailer(tailer, publisher);
        awaitEvents(publisher.events, 1, 3000);
        t.interrupt();
        t.join(2000);

        assertEquals(1, publisher.events.size());
    }

    // ---- log file rotation ----

    @Test
    void tail_detects_rotation_and_offersToQueue(@TempDir Path tempDir) throws Exception {
        // Set up: active file exists; then we rename it to .1.log (simulating C++ rotation)
        // and create a new empty active file.
        Path activeFile = tempDir.resolve("app.device.log");
        Files.writeString(activeFile, "{\"type\":\"kernel_event\"}\n");

        LinkedBlockingQueue<Path> queue = new LinkedBlockingQueue<>();
        CursorManager cursorMgr = new CursorManager(tempDir.resolve("cursor.json").toFile());
        CapturingPublisher publisher = new CapturingPublisher();
        LogTailer tailer = new LogTailer(tempDir.toFile(), "app", "device", "gpu-trace", cursorMgr, queue);

        Thread t = startTailer(tailer, publisher);
        // Wait for tailer to read the line and advance the cursor
        awaitEvents(publisher.events, 1, 3000);
        
        // Stop the tailer thread to release the file lock
        t.interrupt();
        t.join(5000);

        // Simulate rotation: rename active → .1.log, create new empty active
        Path rotatedFile = tempDir.resolve("app.device.1.log");
        Files.move(activeFile, rotatedFile);
        Files.createFile(activeFile); // new empty active file

        // Restart tailer to see if it detects the rotation from the persisted cursor
        t = startTailer(tailer, publisher);

        // The tailer should detect rotation and switch to index 1,
        // then drain the rotated file (it's already fully read, offset == length)
        // then offer the rotated path to the queue.
        Path consumed = queue.poll(5, TimeUnit.SECONDS);
        t.interrupt();
        t.join(2000);

        assertNotNull(consumed, "Rotated file should have been offered to the archive queue");
        assertEquals(rotatedFile.toAbsolutePath(), consumed.toAbsolutePath());
    }

    // ---- rotated file gone on restart ----

    @Test
    void tail_rotatedFileGone_fallsBackToActiveFile(@TempDir Path tempDir) throws Exception {
        // Simulate a cursor that points to fileIndex=1 (rotated), but the file no longer exists
        File cursorFile = tempDir.resolve("cursor.json").toFile();
        // Write a cursor that says we were reading rotated file index 1 at offset 0
        java.nio.file.Files.writeString(cursorFile.toPath(), """
            {
              "streams": {
                "app.device": { "fileIndex": 1, "offset": 0 }
              }
            }
            """);

        // Create only the active file (rotated .1.log is gone)
        Path activeFile = tempDir.resolve("app.device.log");
        Files.writeString(activeFile, "{\"type\":\"kernel_event\"}\n");

        CursorManager cursorMgr = new CursorManager(cursorFile);
        CapturingPublisher publisher = new CapturingPublisher();
        LogTailer tailer = new LogTailer(tempDir.toFile(), "app", "device", "gpu-trace", cursorMgr, new LinkedBlockingQueue<>());

        Thread t = startTailer(tailer, publisher);
        awaitEvents(publisher.events, 1, 5000);
        t.interrupt();
        t.join(2000);

        // Should recover and eventually read from the active file
        assertFalse(publisher.events.isEmpty(), "Should fall back to active file after rotated file is gone");
    }

    // ---- A: read compressed (.gz) rotated files ----

    @Test
    void tail_readsGzRotatedFile(@TempDir Path tempDir) throws Exception {
        String content =
            "{\"type\":\"kernel_event\",\"name\":\"k1\"}\n" +
            "{\"type\":\"kernel_event\",\"name\":\"k2\"}\n";
        Path plain = tempDir.resolve("app.device.1.log");
        Files.writeString(plain, content);
        Path gz = tempDir.resolve("app.device.1.log.gz");
        gzip(plain, gz);
        Files.delete(plain);                              // only the .gz remains
        Files.createFile(tempDir.resolve("app.device.log")); // empty active so tailer doesn't just wait

        File cursorFile = tempDir.resolve("cursor.json").toFile();
        Files.writeString(cursorFile.toPath(),
            "{\"streams\":{\"app.device\":{\"fileIndex\":1,\"offset\":0}}}");

        CursorManager cursorMgr = new CursorManager(cursorFile);
        CapturingPublisher publisher = new CapturingPublisher();
        LogTailer tailer = new LogTailer(tempDir.toFile(), "app", "device", "gpu-trace",
                cursorMgr, new LinkedBlockingQueue<>());

        Thread t = startTailer(tailer, publisher);
        awaitEvents(publisher.events, 2, 3000);
        t.interrupt();
        t.join(2000);

        assertEquals(2, publisher.events.size(), "Both lines should be read from the .gz");
    }

    @Test
    void tail_gzResumeFromOffset(@TempDir Path tempDir) throws Exception {
        String l1 = "{\"type\":\"kernel_event\",\"name\":\"k1\"}\n";
        String l2 = "{\"type\":\"kernel_event\",\"name\":\"k2\"}\n";
        Path plain = tempDir.resolve("app.device.1.log");
        Files.writeString(plain, l1 + l2);
        Path gz = tempDir.resolve("app.device.1.log.gz");
        gzip(plain, gz);
        Files.delete(plain);
        Files.createFile(tempDir.resolve("app.device.log"));

        long off = l1.getBytes(StandardCharsets.UTF_8).length; // start after the first line
        File cursorFile = tempDir.resolve("cursor.json").toFile();
        Files.writeString(cursorFile.toPath(),
            "{\"streams\":{\"app.device\":{\"fileIndex\":1,\"offset\":" + off + "}}}");

        CursorManager cursorMgr = new CursorManager(cursorFile);
        CapturingPublisher publisher = new CapturingPublisher();
        LogTailer tailer = new LogTailer(tempDir.toFile(), "app", "device", "gpu-trace",
                cursorMgr, new LinkedBlockingQueue<>());

        Thread t = startTailer(tailer, publisher);
        awaitEvents(publisher.events, 1, 3000);
        t.interrupt();
        t.join(2000);

        assertEquals(1, publisher.events.size(), "Only the line after the offset should be read");
        assertTrue(publisher.events.get(0).data().contains("k2"));
    }

    // ---- D: restart recovers the unread tail after the client compressed it ----

    @Test
    void tail_restartAfterCompression_recoversUnreadTail(@TempDir Path tempDir) throws Exception {
        Path active = tempDir.resolve("app.device.log");
        String k1 = "{\"type\":\"kernel_event\",\"name\":\"k1\"}\n";
        String k2 = "{\"type\":\"kernel_event\",\"name\":\"k2\"}\n";
        Files.writeString(active, k1 + k2);

        File cursorFile = tempDir.resolve("cursor.json").toFile();

        // Run 1: publisher accepts k1, rejects k2 → the agent stays "behind" at the
        // offset after k1, having recorded the file's identity in the cursor.
        RejectingPublisher pub1 = new RejectingPublisher("k2");
        LogTailer tailer1 = new LogTailer(tempDir.toFile(), "app", "device", "gpu-trace",
                new CursorManager(cursorFile), new LinkedBlockingQueue<>());
        Thread t1 = startTailer(tailer1, pub1);
        awaitEvents(pub1.events, 1, 3000);  // k1 published
        Thread.sleep(400);                  // let it attempt+fail k2 and persist the cursor
        t1.interrupt();
        t1.join(2000);

        CursorPosition saved = new CursorManager(cursorFile).get("app.device");
        assertEquals(0, saved.fileIndex());
        assertEquals(k1.getBytes(StandardCharsets.UTF_8).length, saved.offset(),
                "cursor should sit just after k1");
        assertTrue(saved.headSig() != 0L, "file identity (headSig) should be captured");

        // Simulate the client rotating AND immediately compressing: the old active
        // content lands in .1.log.gz, the active file is replaced with fresh content.
        Path gz = tempDir.resolve("app.device.1.log.gz");
        gzip(active, gz);
        Files.delete(active);
        String k3 = "{\"type\":\"kernel_event\",\"name\":\"k3\"}\n";
        Files.writeString(active, k3);

        // Run 2: without A+D the k2 tail (now only inside the .gz) would be lost.
        CapturingPublisher pub2 = new CapturingPublisher();
        LogTailer tailer2 = new LogTailer(tempDir.toFile(), "app", "device", "gpu-trace",
                new CursorManager(cursorFile), new LinkedBlockingQueue<>());
        Thread t2 = startTailer(tailer2, pub2);
        awaitEvents(pub2.events, 2, 5000);  // k2 (recovered from gz) + k3 (new active)
        t2.interrupt();
        t2.join(2000);

        List<String> datas = pub2.events.stream().map(LogWrapper::data).toList();
        assertTrue(datas.stream().anyMatch(d -> d.contains("k2")),
                "k2 must be recovered from the compressed rotated file");
        assertTrue(datas.stream().anyMatch(d -> d.contains("k3")),
                "k3 from the new active file should also be read");
    }
}
