package com.gpuflight.agent.service;

import com.gpuflight.agent.config.ConfigLoader;
import com.gpuflight.agent.model.DiscoveredSession;
import com.gpuflight.agent.util.Delays;
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

    /** Marker file the agent drops in a session dir once every channel has been
     *  fully uploaded. Discovery skips a session that has it, so a re-scan never
     *  re-tails or re-signals an already-finished session. */
    public static final String UPLOADED_MARKER = ".uploaded";

    /** Marker for a session that finished off an orphaned {@code .tmp/} (producer
     *  crashed or was killed and never removed it). Discovery skips it like
     *  UPLOADED_MARKER; an agent with pruning on deletes such a session instead. */
    public static final String FAILED_MARKER = ".failed";

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
                    if (!Delays.sleep(Delays.SESSION_WATCHER_POLL)) break;
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
            if (looksLikeSession(subdir)) {
                // Flat: <folder>/<session_id>/ - embedded gpufl and single-pass
                // `gpufl trace` write the session directly under the watched dir.
                if (!isSettled(subdir)) {
                    result.add(new DiscoveredSession(folder, subdir.getName(), types));
                }
                continue;
            }
            // One grouping level: <folder>/<group>/<session_id>/ - a multi-pass
            // run nests its passes under a "run-<app>-<analysis_id>" folder. Descend
            // one level (and no further) so each pass session is still discovered.
            File[] grandchildren = subdir.listFiles(File::isDirectory);
            if (grandchildren == null) continue;
            Arrays.sort(grandchildren, Comparator.comparing(File::getName));
            for (File leaf : grandchildren) {
                if (leaf.getName().startsWith(".")) continue;
                if (looksLikeSession(leaf) && !isSettled(leaf)) {
                    result.add(new DiscoveredSession(subdir, leaf.getName(), types));
                }
            }
        }
        return result;
    }

    // A settled session (fully uploaded, or finished+marked failed) is skipped by a
    // re-scan so it is never re-tailed or re-signalled.
    private static boolean isSettled(File sessionDir) {
        return new File(sessionDir, UPLOADED_MARKER).exists()
                || new File(sessionDir, FAILED_MARKER).exists();
    }

    // A directory is a session if it directly contains a channel log file
    // (device/scope/system/sass[.N].log[.gz]).
    private static boolean looksLikeSession(File dir) {
        File[] inside = dir.listFiles();
        if (inside == null) return false;
        for (File child : inside) {
            if (child.isFile() &&
                    CHANNEL_FILE_PATTERN.matcher(child.getName()).matches()) {
                return true;
            }
        }
        return false;
    }
}
