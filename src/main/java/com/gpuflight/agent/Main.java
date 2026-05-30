package com.gpuflight.agent;

import com.gpuflight.agent.config.HttpConfig;
import com.gpuflight.agent.config.JsonSettings;
import com.gpuflight.agent.config.KafkaConfig;
import com.gpuflight.agent.config.PublisherConfig;
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

        // Collect all sources: explicit source + auto-discovered from GPUFL_SOURCE_FOLDERS
        List<LogSourceConfig> allSources = new ArrayList<>();
        if (config.source() != null) {
            System.out.println("[agent] Explicit source: folder=" + config.source().folder()
                             + " prefix=" + config.source().filePrefix()
                             + " types=" + config.source().logTypes());
            allSources.add(config.source());
        }
        if (config.sources() != null) {
            for (LogSourceConfig s : config.sources()) {
                System.out.println("[agent] Config source: folder=" + s.folder()
                                 + " prefix=" + s.filePrefix()
                                 + " types=" + s.logTypes());
                allSources.add(s);
            }
        }

        String foldersRaw = resolve(args, "folders", "GPUFL_SOURCE_FOLDERS", null);
        if (foldersRaw != null) {
            for (String folderPath : foldersRaw.split(",")) {
                folderPath = folderPath.trim();
                if (!folderPath.isEmpty()) {
                    allSources.addAll(discoverSources(new File(folderPath)));
                }
            }
        }

        if (allSources.isEmpty()) {
            System.err.println("❌ No log sources configured (set --folder, --folders, or GPUFL_SOURCE_FOLDERS)");
            System.exit(1);
        }

        String cursorFile = resolve(args, "cursor-file", "GPUFL_CURSOR_FILE", "./cursor.json");
        var cursorMgr = new CursorManager(new File(cursorFile));
        var consumedFilesQueue = new LinkedBlockingQueue<Path>();

        String topicPrefix = topicPrefix(config);

        var executor = Executors.newVirtualThreadPerTaskExecutor();
        var deduplicator = new DeviceMetricDeduplicator();

        // Set of "<folder>::<session_id>" keys already being tailed.
        // ConcurrentHashMap.newKeySet() so the watcher thread (below)
        // and the initial spawn can both insert without losing races.
        var startedSessions = java.util.concurrent.ConcurrentHashMap.<String>newKeySet();

        // Helper: spawn the per-channel tailer set for one session. Idempotent —
        // a second call with the same (folder, sid) is a no-op so the periodic
        // re-scan in the watcher below can call it freely.
        java.util.function.BiConsumer<File, LogSourceConfig> spawnSessionTailers = (folder, source) -> {
            String key = folder.getAbsolutePath() + "::" + source.filePrefix();
            if (!startedSessions.add(key)) return;
            System.out.println("[agent] Tailing session: folder=" + folder
                               + " session=" + source.filePrefix()
                               + " types=" + source.logTypes());
            for (String type : source.logTypes()) {
                executor.submit(() -> {
                    // Only the "system" channel carries device_metric_batch events
                    var dedup = "system".equals(type) ? deduplicator : null;
                    var tailer = new LogTailer(folder, source.filePrefix(), type, topicPrefix,
                                               cursorMgr, consumedFilesQueue, dedup);
                    tailer.tail(publisher);
                });
            }
        };

        // Initial spawn from the sources discovered at startup.
        for (LogSourceConfig source : allSources) {
            var folder = new File(source.folder());
            spawnSessionTailers.accept(folder, source);
        }

        // New-session watcher. The agent must notice sessions that
        // start AFTER it booted — without this, a long-running agent
        // would only ship the sessions that existed at startup. We
        // poll the parent folder(s) instead of inotify/ReadDirectoryChangesW
        // for portability: a 2-second poll latency is negligible
        // compared to typical session lifetimes (minutes), and the
        // cost is one stat() per known subdir per tick.
        //
        // Watched folders = the union of:
        //   - source.folder() of every initial source (covers --folder)
        //   - every entry from --folders (already added above)
        // We dedupe by absolute path so a folder watched by multiple
        // sources only polls once.
        var watchedFolders = new java.util.LinkedHashSet<File>();
        for (LogSourceConfig source : allSources) {
            File f = new File(source.folder());
            // Watch the PARENT of the session subdir — that's where
            // new sibling sessions appear. For explicit sources where
            // folder is already the parent, that's the folder itself.
            watchedFolders.add(f);
        }
        if (foldersRaw != null) {
            for (String folderPath : foldersRaw.split(",")) {
                folderPath = folderPath.trim();
                if (!folderPath.isEmpty()) {
                    watchedFolders.add(new File(folderPath));
                }
            }
        }

        for (File watched : watchedFolders) {
            executor.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        for (LogSourceConfig src : discoverSources(watched)) {
                            spawnSessionTailers.accept(new File(src.folder()), src);
                        }
                        Thread.sleep(2_000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        System.err.println("[watcher] error scanning " +
                                           watched + ": " + e.getMessage());
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
            System.out.println("Shutting down…");
            executor.shutdownNow();
            try { publisher.close(); } catch (Exception ignored) {}
        }));

        // Block main thread until SIGTERM/Ctrl+C triggers the shutdown hook.
        new CountDownLatch(1).await();
    }

    // -------------------------------------------------------------------------
    // Config resolution
    // -------------------------------------------------------------------------

    /**
     * Builds AgentConfig from CLI flags and/or environment variables.
     * Resolution order per field: CLI flag → env var → default.
     * Falls back to the bundled local.json when no flags or GPUFL_* vars are present.
     */
    static AgentConfig loadFromArgs(String[] args) {
        return loadFromArgs(args, System.getenv());
    }

    static AgentConfig loadFromArgs(String[] args, Map<String, String> env) {
        boolean hasAnyConfig = Arrays.stream(args).anyMatch(a -> a.startsWith("--"))
            || env.keySet().stream().anyMatch(k -> k.startsWith("GPUFL_"));

        if (!hasAnyConfig) {
            System.out.println("No flags or GPUFL_* env vars found — using bundled local.json");
            return loadClasspathConfig("config/local.json");
        }

        String folder = resolve(args, "folder", "GPUFL_SOURCE_FOLDER", null, env);
        String foldersEnv = resolve(args, "folders", "GPUFL_SOURCE_FOLDERS", null, env);
        if (folder == null && foldersEnv == null) {
            System.err.println("❌ --folder (or env GPUFL_SOURCE_FOLDER) or --folders (or env GPUFL_SOURCE_FOLDERS) is required");
            printUsage();
            System.exit(1);
        }
        String prefix = resolve(args, "prefix", "GPUFL_SOURCE_PREFIX", "gpufl", env);
        String logTypesRaw = resolve(args, "log-types", "GPUFL_LOG_TYPES", null, env);
        List<String> logTypes = logTypesRaw != null
            ? Arrays.stream(logTypesRaw.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList()
            : null; // null → LogSourceConfig compact constructor applies the default
        String type   = require(args, "type",   "GPUFL_PUBLISHER_TYPE", env);

        // Reject the legacy --url / GPUFL_HTTP_URL flag with a migration
        // hint. Removed May 2026 in favor of --host + --api-version (see
        // HttpConfig javadoc). Without this explicit check the user
        // would silently lose the URL value (resolve() would return
        // null for the new --host name) and hit the IllegalArgumentException
        // out of HttpConfig's compact constructor — same outcome but
        // less obvious about WHICH legacy flag is at fault.
        if (resolve(args, "url", "GPUFL_HTTP_URL", null, env) != null) {
            System.err.println("❌ --url / GPUFL_HTTP_URL is no longer supported.");
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
                Long.parseLong(resolve(args, "timeout", "GPUFL_HTTP_TIMEOUT_SEC", "10", env)));
            case "kafka" -> new KafkaConfig(
                require(args, "brokers",        "GPUFL_KAFKA_BROKERS", env),
                resolve(args, "topic-prefix",   "GPUFL_KAFKA_TOPIC_PREFIX", null, env),
                resolve(args, "compression",    "GPUFL_KAFKA_COMPRESSION", null, env),
                Integer.parseInt(resolve(args, "kafka-linger-ms", "GPUFL_KAFKA_LINGER_MS", "0", env)));
            default -> {
                System.err.println("❌ Unknown publisher type: '" + type + "' (expected: http, kafka)");
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
            ? new LogSourceConfig(folder, prefix, logTypes) : null;
        return new AgentConfig(source, null, publisher, archiver);
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
            System.err.println("❌ --" + flag + " (or env " + envVar + ") is required");
            printUsage();
            System.exit(1);
        }
        return val;
    }

    /**
     * Per-session-subdirectory channel files under v1.2:
     *   <folder>/<sessionId>/{device,scope,system}.log[.N.log[.gz]]
     *
     * Pattern matches the filenames INSIDE a session subdir (no prefix
     * component). Used as a quick check that a candidate subdir looks
     * like a gpufl session (vs. an unrelated user-created folder).
     */
    private static final Pattern CHANNEL_FILE_PATTERN =
        Pattern.compile("^(device|scope|system)(?:\\.\\d+)?\\.log(?:\\.gz)?$");

    /**
     * v1.2: scan {@code folder} for session subdirectories. Each subdir
     * is a session — its name is the session_id; its contents are
     * channel files matching {@link #CHANNEL_FILE_PATTERN}. Returns one
     * {@link LogSourceConfig} per discovered session, with the source's
     * {@code filePrefix} field repurposed to carry the session_id.
     *
     * <p>Subdirectories that don't look like sessions (no channel
     * files inside) are silently skipped. Files at the top level of
     * {@code folder} that match the pre-v1.2 flat pattern
     * {@code <prefix>.<channel>.log} trigger a one-line warning so a
     * user upgrading from v1.1 sees the migration hint.
     */
    static List<LogSourceConfig> discoverSources(File folder) {
        List<LogSourceConfig> result = new ArrayList<>();
        if (!folder.isDirectory()) return result;

        // Legacy-format warning: if there are pre-v1.2 flat files at
        // the folder's top level, the agent won't see them — the new
        // discovery walks subdirs only.
        Pattern legacyTop = Pattern.compile(
            "^(.+)\\.(device|scope|system)(?:\\.\\d+)?\\.log(?:\\.gz)?$");
        File[] topFiles = folder.listFiles();
        if (topFiles != null) {
            for (File f : topFiles) {
                if (f.isFile() && legacyTop.matcher(f.getName()).matches()) {
                    System.err.println(
                        "[agent] warning: found pre-v1.2 flat log file '" +
                        f.getName() + "' at top level of " + folder +
                        ". v1.2 expects <session_id>/<channel>.log " +
                        "under that folder. Re-run with a v1.2 gpufl client " +
                        "or migrate old files into a session subdirectory.");
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
            System.out.println("[agent] Discovered session \"" + subdir.getName() +
                               "\" in " + folder);
            // filePrefix carries the session_id — see LogTailer's
            // constructor, which treats the second arg as session_id
            // (renamed from the v1.1 filePrefix).
            result.add(new LogSourceConfig(folder.getAbsolutePath(),
                                            subdir.getName(), null));
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
            System.err.println("❌ Config file not found: '" + path + "'");
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
                System.err.println("❌ Bundled config not found: '" + resourcePath + "'");
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
              --prefix=<name>              Log file prefix          [GPUFL_SOURCE_PREFIX]  default: gpufl
              --folders=<p1,p2,...>        Auto-discover folders    [GPUFL_SOURCE_FOLDERS]
              --log-types=<a,b,...>        Log channels to tail     [GPUFL_LOG_TYPES]      default: device,scope,system
              --cursor-file=<path>         Cursor state file        [GPUFL_CURSOR_FILE]    default: ./cursor.json

            Publisher (required):
              --type=<http|kafka>          Publisher type           [GPUFL_PUBLISHER_TYPE]

            HTTP publisher:
              --host=<scheme://host[:port]> Backend host             [GPUFL_HTTP_HOST]          e.g. https://api.gpuflight.com
              --api-version=<v1|v2|...>    Backend API version       [GPUFL_HTTP_API_VERSION]   default: v1
              --token=<token>              Bearer auth token         [GPUFL_HTTP_TOKEN]
              --timeout=<seconds>          Request timeout           [GPUFL_HTTP_TIMEOUT_SEC]   default: 10

            Kafka publisher:
              --brokers=<host:port,...>    Bootstrap servers        [GPUFL_KAFKA_BROKERS]
              --topic-prefix=<prefix>      Topic prefix             [GPUFL_KAFKA_TOPIC_PREFIX]  default: gpu-trace
              --compression=<type>         Compression codec        [GPUFL_KAFKA_COMPRESSION]  default: snappy
              --kafka-linger-ms=<ms>       Producer linger.ms       [GPUFL_KAFKA_LINGER_MS]    default: 100

            Archiver (optional — disabled if --archiver-endpoint is absent):
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

              # No args → uses bundled local.json (development only)
              gpufl-agent
            """);
    }
}
