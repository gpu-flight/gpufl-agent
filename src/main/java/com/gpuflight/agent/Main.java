package com.gpuflight.agent;

import com.gpuflight.agent.config.HttpConfig;
import com.gpuflight.agent.config.JsonSettings;
import com.gpuflight.agent.config.KafkaConfig;
import com.gpuflight.agent.config.PublisherConfig;
import com.gpuflight.agent.config.StreamUploadSettings;
import com.gpuflight.agent.model.AgentConfig;
import com.gpuflight.agent.model.ArchiverConfig;
import com.gpuflight.agent.model.LogSourceConfig;
import com.gpuflight.agent.filter.DeviceMetricDeduplicator;
import com.gpuflight.agent.publisher.Publisher;
import com.gpuflight.agent.publisher.PublisherFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

    static void main(String[] args) throws Exception {
        String configPath = parseConfigArg(args);
        AgentConfig config = configPath != null
            ? loadExternalConfig(configPath)
            : loadFromArgs(args);

        Publisher publisher = PublisherFactory.create(config.publisher());
        System.out.println("[agent] Publisher: " + config.publisher().getClass().getSimpleName());

        // In v1.2 a "source" is just a FOLDER to watch - each run writes its
        // logs under <folder>/<session_id>/<channel>.log[.gz], so sessions are
        // auto-discovered (no per-session config). Collect every folder to
        // watch with its channel filter (logTypes); we discover + tail the
        // sessions under each, both now and on the periodic rescan below.
        var watchedFolders = new java.util.LinkedHashMap<File, List<String>>();
        if (config.source() != null) {
            System.out.println("[agent] Source folder: " + config.source().folder()
                             + " types=" + config.source().logTypes());
            watchedFolders.putIfAbsent(new File(config.source().folder()), config.source().logTypes());
        }
        if (config.sources() != null) {
            for (LogSourceConfig s : config.sources()) {
                System.out.println("[agent] Source folder: " + s.folder()
                                 + " types=" + s.logTypes());
                watchedFolders.putIfAbsent(new File(s.folder()), s.logTypes());
            }
        }

        String foldersRaw = resolve(args, "folders", "GPUFL_SOURCE_FOLDERS", null);
        List<String> folderLogTypes = logTypesOrDefault(args, System.getenv());
        if (foldersRaw != null) {
            for (String folderPath : foldersRaw.split(",")) {
                folderPath = folderPath.trim();
                if (!folderPath.isEmpty()) {
                    watchedFolders.putIfAbsent(new File(folderPath), folderLogTypes);
                }
            }
        }

        if (watchedFolders.isEmpty()) {
            System.err.println("ERROR: No log sources configured (set --folder, --folders, or GPUFL_SOURCE_FOLDERS)");
            System.exit(1);
        }

        String cursorFile = resolve(args, "cursor-file", "GPUFL_CURSOR_FILE", "./cursor.json");
        var cursorMgr = new CursorManager(new File(cursorFile));
        var consumedFilesQueue = new LinkedBlockingQueue<Path>();

        String topicPrefix = topicPrefix(config);
        StreamUploadSettings streamUploadSettings = switch (config.publisher()) {
            case HttpConfig http -> StreamUploadSettings.from(http);
            default -> StreamUploadSettings.DISABLED;
        };
        if (streamUploadSettings.enabled()) {
            System.out.println("[agent] HTTP upload mode: stream"
                + " maxLines=" + streamUploadSettings.maxLines()
                + " maxBytes=" + streamUploadSettings.maxBytes());
        }

        var executor = Executors.newVirtualThreadPerTaskExecutor();
        var deduplicator = new DeviceMetricDeduplicator();

        // Set of "<folder>::<session_id>" keys already being tailed.
        // ConcurrentHashMap.newKeySet() so the watcher threads and the
        // initial spawn can all insert without losing races. A session is
        // announced + spawned exactly once; the 2s rescan is then a no-op for
        // it. (With the compressed-active drain in LogTailer, a finished
        // session's tailers exit on their own once drained - they are never
        // re-watched, so this set also marks "done" for the agent's lifetime.)
        var startedSessions = java.util.concurrent.ConcurrentHashMap.<String>newKeySet();

        // Spawn the per-channel tailer set for one discovered session.
        // Idempotent - a second call for the same (folder, session_id) is a
        // no-op, so the rescan can call it freely without re-announcing.
        java.util.function.Consumer<DiscoveredSession> spawnSessionTailers = (session) -> {
            String key = session.folder().getAbsolutePath() + "::" + session.sessionId();
            if (!startedSessions.add(key)) return;
            System.out.println("[agent] Tailing session \"" + session.sessionId()
                               + "\" in " + session.folder() + " types=" + session.logTypes());
            // Count this session's per-channel tailers. When the LAST one finishes
            // NATURALLY (session drained + every batch accepted), tell the backend the
            // upload is complete so it can finalize without waiting out its grace
            // window. A tailer that exits via interruption (agent shutdown) or never
            // finishes (a channel whose file never appears) does NOT count toward the
            // signal — so a partial upload is never declared complete; the backend's
            // grace path covers those.
            var remaining = new java.util.concurrent.atomic.AtomicInteger(session.logTypes().size());
            String sid = session.sessionId();
            for (String type : session.logTypes()) {
                executor.submit(() -> {
                    // Only the "system" channel carries device_metric_batch events.
                    var dedup = "system".equals(type) ? deduplicator : null;
                    var tailer = new LogTailer(session.folder(), session.sessionId(), type,
                                               topicPrefix, cursorMgr, consumedFilesQueue, dedup,
                                               streamUploadSettings);
                    tailer.tail(publisher);
                    // Reached only when tail() returns. Skip on interruption (agent
                    // shutdown) so an in-flight upload is never declared complete.
                    if (!Thread.currentThread().isInterrupted()
                            && remaining.decrementAndGet() == 0
                            && streamUploadSettings.enabled()) {
                        signalSessionComplete(publisher, sid);
                    }
                });
            }
        };

        // Initial discovery + spawn for every watched folder.
        for (var entry : watchedFolders.entrySet()) {
            for (DiscoveredSession s : discoverSources(entry.getKey(), entry.getValue())) {
                spawnSessionTailers.accept(s);
            }
        }

        // New-session watcher. The agent must notice sessions that start AFTER
        // it booted - without this, a long-running agent would only ship the
        // sessions that existed at startup. We poll each watched folder (2s)
        // instead of inotify/ReadDirectoryChangesW for portability: the latency
        // is negligible vs. typical session lifetimes, and the cost is one
        // listing per folder per tick.
        for (var entry : watchedFolders.entrySet()) {
            File folder = entry.getKey();
            List<String> types = entry.getValue();
            executor.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        for (DiscoveredSession s : discoverSources(folder, types)) {
                            spawnSessionTailers.accept(s);
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

        if (config.archiver() != null) {
            var archiver = new LogArchiver(config.archiver());
            executor.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Path path = consumedFilesQueue.take();
                        String objectKey = buildArchiveKey(config.archiver().prefix(), path);
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

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            executor.shutdownNow();
            try { publisher.close(); } catch (Exception ignored) {}
        }));

        // Block main thread until SIGTERM/Ctrl+C triggers the shutdown hook.
        new CountDownLatch(1).await();
    }

    /**
     * Tell the backend that every channel of a session has finished uploading.
     * Best-effort with a few bounded retries: {@code publishSessionComplete}
     * returns true for terminal outcomes (2xx, or a 4xx like 404 from an older
     * backend) and false only for transient 5xx/network errors. If it never gets
     * through, the backend's grace path still finalizes the session, so this is
     * a pure latency optimization — never a correctness dependency.
     */
    private static void signalSessionComplete(Publisher publisher, String sessionId) {
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
        System.out.println("[agent] session-complete signal gave up for " + sessionId
                           + " - backend grace finalize will apply");
    }

    // -------------------------------------------------------------------------
    // Config resolution
    // -------------------------------------------------------------------------

    /**
     * Builds AgentConfig from CLI flags and/or environment variables.
     * Resolution order per field: CLI flag -> env var -> default.
     * Falls back to the bundled local.json when no flags or GPUFL_* vars are present.
     */
    static AgentConfig loadFromArgs(String[] args) {
        return loadFromArgs(args, System.getenv());
    }

    static AgentConfig loadFromArgs(String[] args, Map<String, String> env) {
        boolean hasAnyConfig = Arrays.stream(args).anyMatch(a -> a.startsWith("--"))
            || env.keySet().stream().anyMatch(k -> k.startsWith("GPUFL_"));

        if (!hasAnyConfig) {
            System.out.println("No flags or GPUFL_* env vars found - using bundled local.json");
            return loadClasspathConfig("config/local.json");
        }

        String folder = resolve(args, "folder", "GPUFL_SOURCE_FOLDER", null, env);
        String foldersEnv = resolve(args, "folders", "GPUFL_SOURCE_FOLDERS", null, env);
        if (folder == null && foldersEnv == null) {
            System.err.println("ERROR: --folder (or env GPUFL_SOURCE_FOLDER) or --folders (or env GPUFL_SOURCE_FOLDERS) is required");
            printUsage();
            System.exit(1);
        }
        String logTypesRaw = resolve(args, "log-types", "GPUFL_LOG_TYPES", null, env);
        List<String> logTypes = parseLogTypes(logTypesRaw); // null -> LogSourceConfig applies the default
        String type   = require(args, "type",   "GPUFL_PUBLISHER_TYPE", env);

        // Reject the legacy --url / GPUFL_HTTP_URL flag with a migration
        // hint. Removed May 2026 in favor of --host + --api-version (see
        // HttpConfig javadoc). Without this explicit check the user
        // would silently lose the URL value (resolve() would return
        // null for the new --host name) and hit the IllegalArgumentException
        // out of HttpConfig's compact constructor - same outcome but
        // less obvious about WHICH legacy flag is at fault.
        if (resolve(args, "url", "GPUFL_HTTP_URL", null, env) != null) {
            System.err.println("ERROR: --url / GPUFL_HTTP_URL is no longer supported.");
            System.err.println("   Use --host=<scheme://host[:port]>      (or env GPUFL_HTTP_HOST)");
            System.err.println("       --api-version=<v1|v2|...>          (or env GPUFL_HTTP_API_VERSION, default: v1)");
            System.err.println("   The /api/{version}/events/ path is now built automatically.");
            System.exit(1);
        }
        PublisherConfig publisher = switch (type.toLowerCase()) {
            case "http"  -> new HttpConfig(
                require(args, "host",          "GPUFL_HTTP_HOST", env),
                resolve(args, "api-version",   "GPUFL_HTTP_API_VERSION", HttpConfig.DEFAULT_API_VERSION, env),
                resolve(args, "token",         "GPUFL_HTTP_TOKEN", null, env),
                Long.parseLong(resolve(args, "timeout", "GPUFL_HTTP_TIMEOUT_SEC", "10", env)),
                resolve(args, "upload-mode",   "GPUFL_AGENT_UPLOAD_MODE", HttpConfig.DEFAULT_UPLOAD_MODE, env),
                Integer.parseInt(resolve(args, "stream-max-lines", "GPUFL_AGENT_STREAM_MAX_LINES", "0", env)),
                Long.parseLong(resolve(args, "stream-max-bytes", "GPUFL_AGENT_STREAM_MAX_BYTES", "0", env)));
            case "kafka" -> new KafkaConfig(
                require(args, "brokers",        "GPUFL_KAFKA_BROKERS", env),
                resolve(args, "topic-prefix",   "GPUFL_KAFKA_TOPIC_PREFIX", null, env),
                resolve(args, "compression",    "GPUFL_KAFKA_COMPRESSION", null, env),
                Integer.parseInt(resolve(args, "kafka-linger-ms", "GPUFL_KAFKA_LINGER_MS", "0", env)));
            default -> {
                System.err.println("ERROR: Unknown publisher type: '" + type + "' (expected: http, kafka)");
                printUsage();
                System.exit(1);
                yield null; // unreachable
            }
        };

        ArchiverConfig archiver = null;
        String archiverEndpoint = resolve(args, "archiver-endpoint", "GPUFL_ARCHIVER_ENDPOINT", null, env);
        if (archiverEndpoint != null) {
            archiver = new ArchiverConfig(
                archiverEndpoint,
                require(args, "archiver-bucket",     "GPUFL_ARCHIVER_BUCKET", env),
                resolve(args, "archiver-region",     "GPUFL_ARCHIVER_REGION", null, env),
                require(args, "archiver-access-key", "GPUFL_ARCHIVER_ACCESS_KEY", env),
                require(args, "archiver-secret-key", "GPUFL_ARCHIVER_SECRET_KEY", env),
                resolve(args, "archiver-prefix",     "GPUFL_ARCHIVER_PREFIX", null, env),
                Boolean.parseBoolean(resolve(args, "archiver-delete", "GPUFL_ARCHIVER_DELETE", "false", env)));
        }

        LogSourceConfig source = folder != null
            ? new LogSourceConfig(folder, logTypes) : null;
        return new AgentConfig(source, null, publisher, archiver);
    }

    static List<String> parseLogTypes(String raw) {
        if (raw == null) return null;
        List<String> parsed = Arrays.stream(raw.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .toList();
        return parsed.isEmpty() ? null : parsed;
    }

    static List<String> logTypesOrDefault(String[] args, Map<String, String> env) {
        List<String> parsed = parseLogTypes(resolve(args, "log-types", "GPUFL_LOG_TYPES", null, env));
        return parsed == null ? DEFAULT_LOG_TYPES : parsed;
    }

    /**
     * Resolves a config value: checks --flag=value in args first, then the env var, then the default.
     */
    static String resolve(String[] args, String flag, String envVar, String defaultValue) {
        return resolve(args, flag, envVar, defaultValue, System.getenv());
    }

    static String resolve(String[] args, String flag, String envVar, String defaultValue, Map<String, String> env) {
        String prefix = "--" + flag + "=";
        for (String arg : args) {
            if (arg.startsWith(prefix)) return arg.substring(prefix.length());
        }
        String envVal = env.get(envVar);
        if (envVal != null && !envVal.isBlank()) return envVal;
        return defaultValue;
    }

    /** Like resolve(), but exits with an error if the value is absent. */
    static String require(String[] args, String flag, String envVar) {
        return require(args, flag, envVar, System.getenv());
    }

    static String require(String[] args, String flag, String envVar, Map<String, String> env) {
        String val = resolve(args, flag, envVar, null, env);
        if (val == null) {
            System.err.println("ERROR: --" + flag + " (or env " + envVar + ") is required");
            printUsage();
            System.exit(1);
        }
        return val;
    }

    /**
     * Per-session-subdirectory channel files under v1.2:
     *   <folder>/<sessionId>/{device,scope,system,sass}.log[.N.log[.gz]]
     *
     * Pattern matches the filenames INSIDE a session subdir (no prefix
     * component). Used as a quick check that a candidate subdir looks
     * like a gpufl session (vs. an unrelated user-created folder).
     */
    private static final Pattern CHANNEL_FILE_PATTERN =
        Pattern.compile("^(device|scope|system|sass)(?:\\.\\d+)?\\.log(?:\\.gz)?$");

    /** Folders we've already emitted the pre-v1.2 migration warning for.
     *  {@link #discoverSources} runs on a 2-second rescan loop, so without
     *  this it would re-print the same hint every cycle for as long as the
     *  stale flat files sit in the folder. Warn once per folder instead. */
    private static final java.util.Set<String> warnedLegacyFolders =
        java.util.concurrent.ConcurrentHashMap.newKeySet();

    /** Default channels to tail when a source doesn't restrict them.
     *  "sass" carries the bulky SASS-disassembly / source-content artifacts
     *  split out of device.log; it must be tailed or those artifacts miss
     *  live upload. */
    private static final List<String> DEFAULT_LOG_TYPES = List.of("device", "scope", "system", "sass");

    /** A session found under a watched folder: the folder, the session_id
     *  (the subdir name), and the channels to tail. Carries the session_id
     *  that the pre-v1.2 LogSourceConfig.filePrefix used to overload. */
    record DiscoveredSession(File folder, String sessionId, List<String> logTypes) {}

    /**
     * v1.2: scan {@code folder} for session subdirectories. Each subdir is a
     * session - its name is the session_id; its contents are channel files
     * matching {@link #CHANNEL_FILE_PATTERN}. Returns one
     * {@link DiscoveredSession} per discovered session, each carrying the
     * given {@code logTypes} (or the default channels when null/empty).
     *
     * <p>Subdirectories that don't look like sessions (no channel files
     * inside) are silently skipped. Files at the top level of {@code folder}
     * that match the pre-v1.2 flat pattern {@code <prefix>.<channel>.log}
     * trigger a one-time warning so a user upgrading from v1.1 sees the
     * migration hint. The per-session announcement happens once in
     * spawnSessionTailers, NOT here - this method runs on a 2s rescan loop, so
     * logging a "discovered" line per session every tick would just spam.
     */
    static List<DiscoveredSession> discoverSources(File folder, List<String> logTypes) {
        List<DiscoveredSession> result = new ArrayList<>();
        if (!folder.isDirectory()) return result;

        List<String> types = (logTypes == null || logTypes.isEmpty()) ? DEFAULT_LOG_TYPES : logTypes;

        // Legacy-format warning: if there are pre-v1.2 flat files at
        // the folder's top level, the agent won't see them - the new
        // discovery walks subdirs only.
        Pattern legacyTop = Pattern.compile(
            "^(.+)\\.(device|scope|system|sass)(?:\\.\\d+)?\\.log(?:\\.gz)?$");
        File[] topFiles = folder.listFiles();
        if (topFiles != null) {
            for (File f : topFiles) {
                if (f.isFile() && legacyTop.matcher(f.getName()).matches()) {
                    // Warn only the first time we see legacy files in this
                    // folder - the rescan loop calls this every 2s.
                    if (warnedLegacyFolders.add(folder.getAbsolutePath())) {
                        System.err.println(
                            "[agent] warning: found pre-v1.2 flat log file '" +
                            f.getName() + "' at top level of " + folder +
                            ". v1.2 expects <session_id>/<channel>.log " +
                            "under that folder. Re-run with a v1.2 gpufl client " +
                            "or migrate old files into a session subdirectory.");
                    }
                    break;  // one warning is enough
                }
            }
        }

        File[] subdirs = folder.listFiles(File::isDirectory);
        if (subdirs == null) return result;
        java.util.Arrays.sort(subdirs, java.util.Comparator.comparing(File::getName));

        for (File subdir : subdirs) {
            if (subdir.getName().startsWith(".")) continue;  // skip dotdirs
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

    static String parseConfigArg(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--config=")) return arg.substring("--config=".length());
        }
        return null;
    }

    static String buildArchiveKey(String prefix, Path path) {
        return prefix + path.toFile().getName();
    }

    /** Derives the topic/topic-prefix from whichever publisher is configured. */
    static String topicPrefix(AgentConfig config) {
        return switch (config.publisher()) {
            case KafkaConfig kafka -> kafka.topicPrefix();
            case HttpConfig ignored -> "gpu-trace";
        };
    }

    // -------------------------------------------------------------------------
    // Loaders
    // -------------------------------------------------------------------------

    private static AgentConfig loadExternalConfig(String path) {
        var file = new File(path);
        if (!file.exists()) {
            System.err.println("ERROR: Config file not found: '" + path + "'");
            System.exit(1);
        }
        try {
            return JsonSettings.MAPPER.readValue(file, AgentConfig.class);
        } catch (Exception e) {
            System.err.println("Failed to parse config file: " + e.getMessage());
            System.exit(1);
            return null; // unreachable
        }
    }

    private static AgentConfig loadClasspathConfig(String resourcePath) {
        try (var stream = Main.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (stream == null) {
                System.err.println("ERROR: Bundled config not found: '" + resourcePath + "'");
                System.exit(1);
            }
            return JsonSettings.MAPPER.readValue(stream, AgentConfig.class);
        } catch (Exception e) {
            System.err.println("Failed to parse bundled config: " + e.getMessage());
            System.exit(1);
            return null; // unreachable
        }
    }

    // -------------------------------------------------------------------------
    // Usage
    // -------------------------------------------------------------------------

    static void printUsage() {
        System.err.println("""

            Usage: gpufl-agent [options]

            Config file (overrides all flags):
              --config=<path>              Load full JSON config from file

            Source (at least one of --folder or --folders is required):
              --folder=<path>              Log folder path          [GPUFL_SOURCE_FOLDER]
              --folders=<p1,p2,...>        Auto-discover folders    [GPUFL_SOURCE_FOLDERS]
              --log-types=<a,b,...>        Log channels to tail     [GPUFL_LOG_TYPES]      default: device,scope,system,sass
              --cursor-file=<path>         Cursor state file        [GPUFL_CURSOR_FILE]    default: ./cursor.json

            Publisher (required):
              --type=<http|kafka>          Publisher type           [GPUFL_PUBLISHER_TYPE]

            HTTP publisher:
              --host=<scheme://host[:port]> Backend host             [GPUFL_HTTP_HOST]          e.g. https://api.gpuflight.com
              --api-version=<v1|v2|...>    Backend API version       [GPUFL_HTTP_API_VERSION]   default: v1
              --token=<token>              Bearer auth token         [GPUFL_HTTP_TOKEN]
              --timeout=<seconds>          Request timeout           [GPUFL_HTTP_TIMEOUT_SEC]   default: 10
              --upload-mode=<stream|legacy> Upload protocol           [GPUFL_AGENT_UPLOAD_MODE] default: stream
              --stream-max-lines=<n>       Stream batch line limit    [GPUFL_AGENT_STREAM_MAX_LINES] default: 5000
              --stream-max-bytes=<n>       Stream batch byte limit    [GPUFL_AGENT_STREAM_MAX_BYTES] default: 1000000

            Kafka publisher:
              --brokers=<host:port,...>    Bootstrap servers        [GPUFL_KAFKA_BROKERS]
              --topic-prefix=<prefix>      Topic prefix             [GPUFL_KAFKA_TOPIC_PREFIX]  default: gpu-trace
              --compression=<type>         Compression codec        [GPUFL_KAFKA_COMPRESSION]  default: snappy
              --kafka-linger-ms=<ms>       Producer linger.ms       [GPUFL_KAFKA_LINGER_MS]    default: 100

            Archiver (optional - disabled if --archiver-endpoint is absent):
              --archiver-endpoint=<url>    S3-compatible endpoint   [GPUFL_ARCHIVER_ENDPOINT]
              --archiver-bucket=<name>     S3 bucket                [GPUFL_ARCHIVER_BUCKET]
              --archiver-region=<region>   Region                   [GPUFL_ARCHIVER_REGION]  default: nyc3
              --archiver-access-key=<key>  Access key               [GPUFL_ARCHIVER_ACCESS_KEY]
              --archiver-secret-key=<key>  Secret key               [GPUFL_ARCHIVER_SECRET_KEY]
              --archiver-prefix=<path>     Object key prefix        [GPUFL_ARCHIVER_PREFIX]  default: raw-events/
              --archiver-delete=<bool>     Delete after upload      [GPUFL_ARCHIVER_DELETE]  default: false

            Examples:
              # CLI flags
              gpufl-agent --folder=/var/log/gpuflight --type=http --host=https://api.gpuflight.com

              # Env vars (systemd / Docker)
              GPUFL_SOURCE_FOLDER=/var/log GPUFL_PUBLISHER_TYPE=http GPUFL_HTTP_HOST=https://api.gpuflight.com gpufl-agent

              # JSON config file
              gpufl-agent --config=/etc/gpuflight/agent.json

              # No args -> uses bundled local.json (development only)
              gpufl-agent
            """);
    }
}
