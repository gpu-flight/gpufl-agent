package com.gpuflight.agent.service;

import com.gpuflight.agent.CursorManager;
import com.gpuflight.agent.LogTailer;
import com.gpuflight.agent.SessionOwnership;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

    private final Map<String, SessionRun> sessions = new ConcurrentHashMap<>();
    private final AtomicInteger activeTailers = new AtomicInteger(0);
    private volatile boolean pruneFailed = false;

    /** One session's tailers. Its monitor guards the channel set and the completion
     *  decision, since the watcher can add a channel while the last tailer completes. */
    private static final class SessionRun {
        final DiscoveredSession session;
        final File dir;
        final Set<String> channels = new HashSet<>();
        final AtomicBoolean orphaned = new AtomicBoolean(false);
        int running;
        boolean overCap;
        boolean completing;

        SessionRun(DiscoveredSession session) {
            this.session = session;
            this.dir = new File(session.folder(), session.sessionId());
        }
    }

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

    /**
     * Starts tailing this session once, and later tails each channel found in it that
     * has no tailer yet: a channel's first window can be published while the session
     * is live.
     *
     * @return {@code true} only when this invocation submitted tailers, for a new session
     *     or a new channel; {@code false} when an asynchronous watcher or a terminal-drain
     *     rescan had already started the session and every channel found in it.
     */
    public boolean spawnSessionTailers(DiscoveredSession session) {
        String key = session.folder().getAbsolutePath() + "::" + session.sessionId();
        SessionRun created = new SessionRun(session);
        SessionRun run = sessions.putIfAbsent(key, created);
        if (run == null) {
            run = created;
            log.info("Tailing session \"{}\" in {} channels={}",
                    session.sessionId(), session.folder(), session.logTypes());
        }
        synchronized (run) {
            List<String> added = tailNewChannels(run, session.logTypes());
            if (run != created && !added.isEmpty()) {
                log.info("Tailing channels {} published in live session \"{}\"",
                        added, session.sessionId());
            }
            return run == created || !added.isEmpty();
        }
    }

    // Starts a tailer for each channel in found that has none yet, up to the per-session
    // cap. The caller holds the run's monitor.
    private List<String> tailNewChannels(SessionRun run, List<String> found) {
        List<String> added = new ArrayList<>();
        if (run.completing) return added;
        for (String channel : found) {
            if (run.channels.contains(channel)) continue;
            if (run.channels.size() >= SessionWatcher.MAX_CHANNELS_PER_SESSION) {
                if (!run.overCap) {
                    log.error("session {} has more than {} channels; channel {} and any "
                                    + "later one are not uploaded and the session will "
                                    + "not be reported complete", run.session.sessionId(),
                            SessionWatcher.MAX_CHANNELS_PER_SESSION, channel);
                }
                run.overCap = true;
                break;
            }
            run.channels.add(channel);
            added.add(channel);
            startTailer(run, channel);
        }
        return added;
    }

    private void startTailer(SessionRun run, String type) {
        run.running++;
        activeTailers.incrementAndGet();
        executor.submit(() -> {
            try {
                var dedup = "system".equals(type) ? deduplicator : null;
                var tailer = new LogTailer(run.session.folder(), run.session.sessionId(), type,
                        topicPrefix, cursorMgr, consumedFilesQueue, dedup,
                        streamUploadSettings);
                if (tailer.tail(publisher)) run.orphaned.set(true);  // finished off a stale .tmp/
                if (!Thread.currentThread().isInterrupted()) channelFinished(run);
            } finally {
                activeTailers.decrementAndGet();
            }
        });
    }

    private void channelFinished(SessionRun run) {
        synchronized (run) {
            if (--run.running > 0) return;
            // Every tailer has seen the producer finish. A channel whose first window it
            // published just before that has no tailer if the watcher has not polled
            // since, so look once more; its tailer completes the session instead.
            List<String> late = tailNewChannels(run, SessionWatcher.discoverChannels(
                    run.dir, run.session.channelFilter()));
            if (!late.isEmpty()) {
                log.info("Tailing channels {} found in session \"{}\" before completing it",
                        late, run.session.sessionId());
                return;
            }
            run.completing = true;
        }
        String sid = run.session.sessionId();
        boolean transportLoss = SessionOwnership.hasTransportLoss(run.dir.toPath());
        if (transportLoss) {
            run.orphaned.set(true);
            log.error("session {} has a durable transport-loss "
                    + "marker; refusing session-complete", sid);
        }
        if (run.overCap) {
            log.error("session {} has channels that were not uploaded; "
                    + "refusing session-complete", sid);
        }
        // Last channel done: every available window is uploaded. Signal the
        // backend (stream mode), then settle the session so a re-scan skips it.
        if (streamUploadSettings.enabled() && !transportLoss && !run.overCap) {
            signalSessionComplete(publisher, sid);
        }
        settleSession(run.dir, run.orphaned.get() || run.overCap, run.overCap);
    }

    /** Settle a finished session so a later scan skips it. A producer that closed
     *  cleanly (.tmp/ removed) is marked {@code .uploaded}; one finished off a stale
     *  .tmp/ (producer crashed or was killed) is marked {@code .failed} - or deleted
     *  when pruning is on, since its windows are already uploaded. A session still
     *  holding windows that were never sent (over the channel cap) is never pruned.
     *  The marker's presence is the signal, so an empty file is enough. */
    private void settleSession(File sessionDir, boolean orphaned, boolean holdsUnsentWindows) {
        if (orphaned && pruneFailed && !holdsUnsentWindows) {
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
        return !sessions.isEmpty();
    }
}
