package com.gpuflight.agent.service;

import com.gpuflight.agent.CursorManager;
import com.gpuflight.agent.LogTailer;
import com.gpuflight.agent.config.StreamUploadSettings;
import com.gpuflight.agent.filter.DeviceMetricDeduplicator;
import com.gpuflight.agent.model.DiscoveredSession;
import com.gpuflight.agent.publisher.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
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

        var remaining = new AtomicInteger(session.logTypes().size());
        String sid = session.sessionId();
        for (String type : session.logTypes()) {
            activeTailers.incrementAndGet();
            executor.submit(() -> {
                try {
                    var dedup = "system".equals(type) ? deduplicator : null;
                    var tailer = new LogTailer(session.folder(), session.sessionId(), type,
                            topicPrefix, cursorMgr, consumedFilesQueue, dedup,
                            streamUploadSettings);
                    tailer.tail(publisher);
                    if (!Thread.currentThread().isInterrupted()
                            && remaining.decrementAndGet() == 0
                            && streamUploadSettings.enabled()) {
                        signalSessionComplete(publisher, sid);
                    }
                } finally {
                    activeTailers.decrementAndGet();
                }
            });
        }
    }

    private void signalSessionComplete(Publisher publisher, String sessionId) {
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                if (publisher.publishSessionComplete(sessionId)) return;
            } catch (Exception e) {
                System.err.println("[agent] session-complete signal error for "
                        + sessionId + ": " + e.getMessage());
            }
            try {
                Thread.sleep(2000L);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        log.warn("session-complete signal gave up for {} - backend grace finalize will apply", sessionId);
    }

    public AtomicInteger getActiveTailers() {
        return activeTailers;
    }
}
