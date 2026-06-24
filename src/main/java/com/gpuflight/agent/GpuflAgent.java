package com.gpuflight.agent;

import com.gpuflight.agent.config.ConfigLoader;
import com.gpuflight.agent.config.HttpConfig;
import com.gpuflight.agent.config.KafkaConfig;
import com.gpuflight.agent.config.StreamUploadSettings;
import com.gpuflight.agent.filter.DeviceMetricDeduplicator;
import com.gpuflight.agent.model.AgentConfig;
import com.gpuflight.agent.model.DiscoveredSession;
import com.gpuflight.agent.model.LogSourceConfig;
import com.gpuflight.agent.publisher.Publisher;
import com.gpuflight.agent.publisher.PublisherFactory;
import com.gpuflight.agent.service.SessionWatcher;
import com.gpuflight.agent.service.TailerManager;
import com.gpuflight.agent.util.Delays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class GpuflAgent {

    private static final Logger log = LoggerFactory.getLogger(GpuflAgent.class);

    private final AgentConfig config;
    private final String[] args;
    private final Map<String, String> env;

    private ExecutorService executor;
    private Publisher publisher;
    private TailerManager tailerManager;
    private final Map<File, List<String>> watchedFolders = new LinkedHashMap<>();

    public GpuflAgent(AgentConfig config, String[] args, Map<String, String> env) {
        this.config = config;
        this.args = args;
        this.env = env;
    }

    public void start() throws Exception {
        publisher = PublisherFactory.create(config.publisher());
        log.info("Publisher: {}", config.publisher().getClass().getSimpleName());

        boolean exitWhenDrained = ConfigLoader.parseExitWhenDrained(args, env);

        resolveWatchedFolders();

        if (watchedFolders.isEmpty()) {
            System.err.println("ERROR: No log sources configured (set --folder, --folders, or GPUFL_SOURCE_FOLDERS)");
            System.exit(1);
        }

        String cursorFile = ConfigLoader.resolve(args, "cursor-file", "GPUFL_CURSOR_FILE", "./cursor.json", env);
        var cursorMgr = new CursorManager(new File(cursorFile));
        var consumedFilesQueue = new LinkedBlockingQueue<Path>();

        String topicPrefix = topicPrefix(config);
        StreamUploadSettings streamUploadSettings = switch (config.publisher()) {
            case HttpConfig http -> StreamUploadSettings.from(http);
            default -> StreamUploadSettings.DISABLED;
        };
        if (streamUploadSettings.enabled()) {
            log.info("HTTP upload mode: stream maxLines={} maxBytes={}",
                    streamUploadSettings.maxLines(), streamUploadSettings.maxBytes());
        }

        executor = Executors.newVirtualThreadPerTaskExecutor();
        var deduplicator = new DeviceMetricDeduplicator();

        tailerManager = new TailerManager(executor, publisher, cursorMgr, consumedFilesQueue,
                deduplicator, streamUploadSettings, topicPrefix);

        // Initial discovery + spawn
        for (var entry : watchedFolders.entrySet()) {
            for (DiscoveredSession s : SessionWatcher.discoverSources(entry.getKey(), entry.getValue())) {
                tailerManager.spawnSessionTailers(s);
            }
        }

        // Start watchers
        for (var entry : watchedFolders.entrySet()) {
            new SessionWatcher(entry.getKey(), entry.getValue(), tailerManager::spawnSessionTailers)
                    .start(executor);
        }

        if (config.archiver() != null) {
            var archiver = new LogArchiver(config.archiver());
            executor.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Path path = consumedFilesQueue.take();
                        String objectKey = ConfigLoader.buildArchiveKey(config.archiver().prefix(), path);
                        archiver.archive(path, objectKey);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        System.err.println("[archiver] Error: " + e.getMessage());
                    }
                }
            });
        }

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));

        if (exitWhenDrained) {
            log.info("exit-when-drained mode enabled");
            awaitDrainThenExit(watchedFolders.keySet(), tailerManager);
            log.info("all sessions drained - exiting");
            shutdown();
            return;
        }

        // Daemon mode
        new CountDownLatch(1).await();
    }

    private void resolveWatchedFolders() {
        if (config.source() != null) {
            log.info("Source folder: {} types={}", config.source().folder(), config.source().logTypes());
            watchedFolders.putIfAbsent(new File(config.source().folder()), config.source().logTypes());
        }
        if (config.sources() != null) {
            for (LogSourceConfig s : config.sources()) {
                log.info("Source folder: {} types={}", s.folder(), s.logTypes());
                watchedFolders.putIfAbsent(new File(s.folder()), s.logTypes());
            }
        }

        String foldersRaw = ConfigLoader.resolve(args, "folders", "GPUFL_SOURCE_FOLDERS", null, env);
        List<String> folderLogTypes = ConfigLoader.logTypesOrDefault(args, env);
        if (foldersRaw != null) {
            for (String folderPath : foldersRaw.split(",")) {
                folderPath = folderPath.trim();
                if (!folderPath.isEmpty()) {
                    watchedFolders.putIfAbsent(new File(folderPath), folderLogTypes);
                }
            }
        }
    }

    private void shutdown() {
        log.info("Shutting down...");
        if (executor != null) executor.shutdownNow();
        try { if (publisher != null) publisher.close(); } catch (Exception ignored) {}
    }

    void awaitDrainThenExit(Collection<File> folders, TailerManager tailers) {
        int clean = 0;
        while (true) {
            if (!Delays.sleep(Delays.DRAIN_CHECK_POLL)) break;
            // Gate on "a session was discovered" (cumulative), not on observing the live
            // .tmp/ marker: a short trace finalizes that marker between our 1s polls, which
            // left the old sawActive gate spinning forever even after the upload drained.
            boolean started = tailers.hasStartedAnySession();
            boolean idle = tailers.getActiveTailers().get() == 0 && !anyActiveSession(folders);
            clean = (started && idle) ? clean + 1 : 0;
            if (clean >= 2) return;
        }
    }

    static boolean anyActiveSession(Collection<File> folders) {
        for (File folder : folders) {
            File[] subdirs = folder.listFiles(File::isDirectory);
            if (subdirs == null) continue;
            for (File subdir : subdirs) {
                if (subdir.getName().startsWith(".")) continue;
                if (new File(subdir, ".tmp").isDirectory()) return true;
            }
        }
        return false;
    }

    static String topicPrefix(AgentConfig config) {
        return switch (config.publisher()) {
            case KafkaConfig kafka -> kafka.topicPrefix();
            case HttpConfig ignored -> "gpu-trace";
        };
    }
}
