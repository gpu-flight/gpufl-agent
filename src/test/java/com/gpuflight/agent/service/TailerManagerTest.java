package com.gpuflight.agent.service;

import com.gpuflight.agent.CursorManager;
import com.gpuflight.agent.config.StreamUploadSettings;
import com.gpuflight.agent.model.DiscoveredSession;
import com.gpuflight.agent.model.LogWrapper;
import com.gpuflight.agent.publisher.Publisher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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

    private TailerManager managerIn(ExecutorService ex, Path root) {
        return new TailerManager(ex, new NoopPublisher(),
                new CursorManager(new File(root.toFile(), "cursor.json")),
                new LinkedBlockingQueue<>(), null, StreamUploadSettings.DISABLED, "gpu-trace");
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
}
