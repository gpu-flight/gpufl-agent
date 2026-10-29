package com.gpuflight.agent.service;

import com.gpuflight.agent.config.ConfigLoader;
import com.gpuflight.agent.config.JsonSettings;
import com.gpuflight.agent.model.DiscoveredSession;
import com.gpuflight.agent.util.Delays;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;

public class SessionWatcher {

    private static final Logger log = LoggerFactory.getLogger(SessionWatcher.class);

    /** Most channels the agent tails in one session. A producer adds a channel by
     *  publishing a window for it, so this bounds the tailers a broken producer can make
     *  the agent run; a session with more is never reported complete. */
    public static final int MAX_CHANNELS_PER_SESSION = 16;

    // A published window's sidecar, which stays as its tombstone once the payload is
    // ACKed and deleted. The channel name rule is the one the backend's stream endpoint
    // accepts.
    private static final Pattern WINDOW_SIDECAR = Pattern.compile(
            "^\\.gpufl-window\\.([A-Za-z0-9_-]{1,64})\\.[1-9][0-9]*\\.json$");

    // The native channels of clients that predate sidecars are still recognized by their
    // bare log files. No other name is: an application log next to the windows
    // (server.log) must never become a channel.
    private static final Pattern LEGACY_NATIVE_FILE = Pattern.compile(
            "^(device|scope|system|sass)(?:\\.\\d+)?\\.log(?:\\.gz)?$");

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

    /**
     * The unsettled sessions under {@code folder}, each with the channels found in it.
     * {@code logTypes} is the --log-types filter: empty (or null) tails every channel a
     * producer publishes; otherwise only the named ones are tailed, and a directory is a
     * session only if one of them is found in it.
     */
    public static List<DiscoveredSession> discoverSources(File folder, List<String> logTypes) {
        List<DiscoveredSession> result = new ArrayList<>();
        if (!folder.isDirectory()) return result;

        // Recognize a session only by a channel this agent tails. Adopting a directory
        // whose channels all go untailed (a serving-only session under
        // --log-types=device,scope,system,sass) would complete and mark it .uploaded
        // with its windows never sent, so a later agent that does tail them skips it.
        List<String> filter = logTypes == null ? ConfigLoader.DEFAULT_LOG_TYPES : logTypes;

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
            // Only a session is ever settled, so skip one without reading its sidecars.
            if (isSettled(subdir)) continue;
            List<String> channels = discoverChannels(subdir, filter);
            if (!channels.isEmpty()) {
                // Flat: <folder>/<session_id>/ - embedded gpufl and single-pass
                // `gpufl trace` write the session directly under the watched dir.
                result.add(new DiscoveredSession(folder, subdir.getName(), channels, filter));
                continue;
            }
            // One grouping level: <folder>/<group>/<session_id>/ - a multi-pass
            // run nests its passes under a "run-<app>-<analysis_id>" folder. Descend
            // one level (and no further) so each pass session is still discovered.
            File[] grandchildren = subdir.listFiles(File::isDirectory);
            if (grandchildren == null) continue;
            Arrays.sort(grandchildren, Comparator.comparing(File::getName));
            for (File leaf : grandchildren) {
                if (leaf.getName().startsWith(".") || isSettled(leaf)) continue;
                List<String> leafChannels = discoverChannels(leaf, filter);
                if (!leafChannels.isEmpty()) {
                    result.add(new DiscoveredSession(subdir, leaf.getName(), leafChannels, filter));
                }
            }
        }
        return result;
    }

    /**
     * The channels {@code sessionDir} holds, sorted; at most one more than
     * {@link #MAX_CHANNELS_PER_SESSION}, enough to tell that a session is over it. A
     * name is a channel once a window sidecar for it is published (or kept as an ACK
     * tombstone); for the four native channels a bare window file also counts.
     * {@code filter} limits the result to the channels it names, unless it is empty.
     */
    public static List<String> discoverChannels(File sessionDir, List<String> filter) {
        String[] names = sessionDir.list();
        if (names == null) return List.of();
        Arrays.sort(names);
        Set<String> channels = new TreeSet<>();
        for (String name : names) {
            Matcher match = LEGACY_NATIVE_FILE.matcher(name);
            boolean legacy = match.matches();
            if (!legacy) {
                match = WINDOW_SIDECAR.matcher(name);
                if (!match.matches()) continue;
            }
            String channel = match.group(1);
            if (channels.contains(channel)
                    || !(filter.isEmpty() || filter.contains(channel))) continue;
            File file = new File(sessionDir, name);
            if (legacy ? file.isFile() : isWindowSidecar(file)) {
                channels.add(channel);
                if (channels.size() > MAX_CHANNELS_PER_SESSION) break;
            }
        }
        return List.copyOf(channels);
    }

    // A sidecar counts once it parses and carries the payload size and CRC32 the tailer
    // verifies; the tailer checks the rest of its identity before it uploads the window.
    private static boolean isWindowSidecar(File file) {
        try {
            JsonNode sidecar = JsonSettings.MAPPER.readTree(file);
            return sidecar.isObject()
                    && sidecar.path("payload_bytes").isIntegralNumber()
                    && sidecar.path("payload_crc32").isIntegralNumber();
        } catch (RuntimeException unreadable) {
            return false;
        }
    }

    // A settled session (fully uploaded, or finished+marked failed) is skipped by a
    // re-scan so it is never re-tailed or re-signalled.
    private static boolean isSettled(File sessionDir) {
        return new File(sessionDir, UPLOADED_MARKER).exists()
                || new File(sessionDir, FAILED_MARKER).exists();
    }
}
