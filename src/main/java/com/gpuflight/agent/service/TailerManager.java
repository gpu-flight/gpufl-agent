package com.gpuflight.agent.service;

import com.gpuflight.agent.CursorManager;
import com.gpuflight.agent.LogTailer;
import com.gpuflight.agent.config.StreamUploadSettings;
import com.gpuflight.agent.filter.DeviceMetricDeduplicator;
import com.gpuflight.agent.model.DiscoveredSession;
import com.gpuflight.agent.publisher.Publisher;
import com.gpuflight.agent.util.Delays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TailerManager {

    private static final Logger log = LoggerFactory.getLogger(TailerManager.class);

    private final ExecutorService executor;
    private final Publisher publisher;
    private final CursorManager cursorMgr;
    private final BlockingQueue<Path> consumedFilesQueue;
    private final DeviceMetricDeduplicator deduplicator;
    private final StreamUploadSettings streamUploadSettings;
    private final String topicPrefix;

    private final Set<String> startedSessions = ConcurrentHashMap.newKeySet();
    private final AtomicInteger activeTailers = new AtomicInteger(0);
    private volatile boolean pruneFailed = false;

    public TailerManager(ExecutorService executor,
                         Publisher publisher,
                         CursorManager cursorMgr,
                         BlockingQueue<Path> consumedFilesQueue,
                         DeviceMetricDeduplicator deduplicator,
                         StreamUploadSettings streamUploadSettings,
                         String topicPrefix) {
        this.executor = executor;
        this.publisher = publisher;
        this.cursorMgr = cursorMgr;
        this.consumedFilesQueue = consumedFilesQueue;
        this.deduplicator = deduplicator;
        this.streamUploadSettings = streamUploadSettings;
        this.topicPrefix = topicPrefix;
    }

    public void spawnSessionTailers(DiscoveredSession session) {
        String key = session.folder().getAbsolutePath() + "::" + session.sessionId();
        if (!startedSessions.add(key)) return;
        log.info("Tailing session \"{}\" in {} types={}",
                session.sessionId(), session.folder(), session.logTypes());

        File sessionDir = new File(session.folder(), session.sessionId());
        var remaining = new AtomicInteger(session.logTypes().size());
        var orphaned = new AtomicBoolean(false);
        String sid = session.sessionId();
        for (String type : session.logTypes()) {
            activeTailers.incrementAndGet();
            executor.submit(() -> {
                try {
                    var dedup = "system".equals(type) ? deduplicator : null;
                    var tailer = new LogTailer(session.folder(), session.sessionId(), type,
                            topicPrefix, cursorMgr, consumedFilesQueue, dedup,
                            streamUploadSettings);
                    if (tailer.tail(publisher)) orphaned.set(true);  // finished off a stale .tmp/
                    if (!Thread.currentThread().isInterrupted()
                            && remaining.decrementAndGet() == 0) {
                        // Last channel done: every available window is uploaded. Signal the
                        // backend (stream mode), then settle the session so a re-scan skips it.
                        if (streamUploadSettings.enabled()) {
                            signalSessionComplete(publisher, sid);
                        }
                        settleSession(sessionDir, orphaned.get());
                    }
                } finally {
                    activeTailers.decrementAndGet();
                }
            });
        }
    }

    /** Settle a finished session so a later scan skips it. A producer that closed
     *  cleanly (.tmp/ removed) is marked {@code .uploaded}; one finished off a stale
     *  .tmp/ (producer crashed or was killed) is marked {@code .failed} - or deleted
     *  when pruning is on, since its windows are already uploaded. The marker's
     *  presence is the signal, so an empty file is enough. */
    private void settleSession(File sessionDir, boolean orphaned) {
        if (orphaned && pruneFailed) {
            // Windows are already uploaded; drop the local orphan. A locked .tmp/ can
            // block deletion - fall back to marking it failed.
            if (deleteRecursively(sessionDir)) {
                log.info("pruned orphaned session {}", sessionDir.getName());
                return;
            }
            log.warn("could not prune orphaned session {} - marking failed instead", sessionDir.getName());
        }
        String marker = orphaned ? SessionWatcher.FAILED_MARKER : SessionWatcher.UPLOADED_MARKER;
        try {
            Files.writeString(new File(sessionDir, marker).toPath(), "");
        } catch (Exception e) {
            log.warn("could not write {} marker in {}: {}", marker, sessionDir, e.getMessage());
        }
    }

    private static boolean deleteRecursively(File f) {
        File[] kids = f.listFiles();
        if (kids != null) {
            for (File k : kids) deleteRecursively(k);
        }
        return f.delete();
    }

    private void signalSessionComplete(Publisher publisher, String sessionId) {
        long delayMs = Delays.SESSION_COMPLETE_RETRY.toMillis();
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                if (publisher.publishSessionComplete(sessionId)) return;
            } catch (Exception e) {
                log.error("session-complete signal error for {}: {}", sessionId, e.getMessage());
            }
            if (!Delays.sleep(Duration.ofMillis(delayMs))) return;
            delayMs *= 2; // Exponential backoff
        }
        log.warn("session-complete signal gave up for {} - backend grace finalize will apply", sessionId);
    }

    public AtomicInteger getActiveTailers() {
        return activeTailers;
    }

    /** Enable deleting orphaned sessions (finished off a stale .tmp/) instead of
     *  leaving a {@code .failed} marker - their windows are already uploaded. */
    public void setPruneFailed(boolean prune) {
        this.pruneFailed = prune;
    }

    /** True once any session has been discovered and tailed. Cumulative - never cleared. */
    public boolean hasStartedAnySession() {
        return !startedSessions.isEmpty();
    }
}
