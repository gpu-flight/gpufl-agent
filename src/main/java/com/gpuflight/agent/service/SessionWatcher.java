package com.gpuflight.agent.service;

import com.gpuflight.agent.config.ConfigLoader;
import com.gpuflight.agent.model.DiscoveredSession;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionWatcher {

    private static final Logger log = LoggerFactory.getLogger(SessionWatcher.class);

    private static final Pattern CHANNEL_FILE_PATTERN =
            Pattern.compile("^(device|scope|system|sass)(?:\\.\\d+)?\\.log(?:\\.gz)?$");

    private static final Set<String> warnedLegacyFolders =
            ConcurrentHashMap.newKeySet();

    private final File folder;
    private final List<String> logTypes;
    private final Consumer<DiscoveredSession> onSessionDiscovered;

    public SessionWatcher(File folder, List<String> logTypes, Consumer<DiscoveredSession> onSessionDiscovered) {
        this.folder = folder;
        this.logTypes = logTypes;
        this.onSessionDiscovered = onSessionDiscovered;
    }

    public void start(ExecutorService executor) {
        executor.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    for (DiscoveredSession s : discoverSources(folder, logTypes)) {
                        onSessionDiscovered.accept(s);
                    }
                    Thread.sleep(2_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    System.err.println("[watcher] error scanning " +
                            folder + ": " + e.getMessage());
                }
            }
        });
    }

    public static List<DiscoveredSession> discoverSources(File folder, List<String> logTypes) {
        List<DiscoveredSession> result = new ArrayList<>();
        if (!folder.isDirectory()) return result;

        List<String> types = (logTypes == null || logTypes.isEmpty()) ? ConfigLoader.DEFAULT_LOG_TYPES : logTypes;

        // Legacy-format warning
        Pattern legacyTop = Pattern.compile(
                "^(.+)\\.(device|scope|system|sass)(?:\\.\\d+)?\\.log(?:\\.gz)?$");
        File[] topFiles = folder.listFiles();
        if (topFiles != null) {
            for (File f : topFiles) {
                if (f.isFile() && legacyTop.matcher(f.getName()).matches()) {
                    if (warnedLegacyFolders.add(folder.getAbsolutePath())) {
                        System.err.println(
                                "[agent] warning: found pre-v1.2 flat log file '" +
                                        f.getName() + "' at top level of " + folder +
                                        ". v1.2 expects <session_id>/<channel>.log " +
                                        "under that folder. Re-run with a v1.2 gpufl client " +
                                        "or migrate old files into a session subdirectory.");
                    }
                    break;
                }
            }
        }

        File[] subdirs = folder.listFiles(File::isDirectory);
        if (subdirs == null) return result;
        Arrays.sort(subdirs, Comparator.comparing(File::getName));

        for (File subdir : subdirs) {
            if (subdir.getName().startsWith(".")) continue;
            File[] inside = subdir.listFiles();
            if (inside == null) continue;
            boolean looksLikeSession = false;
            for (File child : inside) {
                if (child.isFile() &&
                        CHANNEL_FILE_PATTERN.matcher(child.getName()).matches()) {
                    looksLikeSession = true;
                    break;
                }
            }
            if (!looksLikeSession) continue;
            result.add(new DiscoveredSession(folder, subdir.getName(), types));
        }
        return result;
    }
}
