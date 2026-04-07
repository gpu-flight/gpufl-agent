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

        for (LogSourceConfig source : allSources) {
            var folder = new File(source.folder());
            System.out.println("[agent] Source: folder=" + folder + " prefix=" + source.filePrefix()
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
        boolean hasAnyConfig = Arrays.stream(args).anyMatch(a -> a.startsWith("--"))
            || System.getenv().keySet().stream().anyMatch(k -> k.startsWith("GPUFL_"));

        if (!hasAnyConfig) {
            System.out.println("No flags or GPUFL_* env vars found — using bundled local.json");
            return loadClasspathConfig("config/local.json");
        }

        String folder = resolve(args, "folder", "GPUFL_SOURCE_FOLDER", null);
        String foldersEnv = resolve(args, "folders", "GPUFL_SOURCE_FOLDERS", null);
        if (folder == null && foldersEnv == null) {
            System.err.println("❌ --folder (or env GPUFL_SOURCE_FOLDER) or --folders (or env GPUFL_SOURCE_FOLDERS) is required");
            printUsage();
            System.exit(1);
        }
        String prefix = resolve(args, "prefix", "GPUFL_SOURCE_PREFIX", "gpufl");
        String logTypesRaw = resolve(args, "log-types", "GPUFL_LOG_TYPES", null);
        List<String> logTypes = logTypesRaw != null
            ? Arrays.stream(logTypesRaw.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList()
            : null; // null → LogSourceConfig compact constructor applies the default
        String type   = require(args, "type",   "GPUFL_PUBLISHER_TYPE");

        PublisherConfig publisher = switch (type.toLowerCase()) {
            case "http"  -> new HttpConfig(
                require(args, "url",     "GPUFL_HTTP_URL"),
                resolve(args, "token",   "GPUFL_HTTP_TOKEN", null),
                Long.parseLong(resolve(args, "timeout", "GPUFL_HTTP_TIMEOUT_SEC", "10")));
            case "kafka" -> new KafkaConfig(
                require(args, "brokers",        "GPUFL_KAFKA_BROKERS"),
                resolve(args, "topic-prefix",   "GPUFL_KAFKA_TOPIC_PREFIX", null),
                resolve(args, "compression",    "GPUFL_KAFKA_COMPRESSION", null),
                Integer.parseInt(resolve(args, "kafka-linger-ms", "GPUFL_KAFKA_LINGER_MS", "0")));
            default -> {
                System.err.println("❌ Unknown publisher type: '" + type + "' (expected: http, kafka)");
                printUsage();
                System.exit(1);
                yield null; // unreachable
            }
        };

        ArchiverConfig archiver = null;
        String archiverEndpoint = resolve(args, "archiver-endpoint", "GPUFL_ARCHIVER_ENDPOINT", null);
        if (archiverEndpoint != null) {
            archiver = new ArchiverConfig(
                archiverEndpoint,
                require(args, "archiver-bucket",     "GPUFL_ARCHIVER_BUCKET"),
                resolve(args, "archiver-region",     "GPUFL_ARCHIVER_REGION", null),
                require(args, "archiver-access-key", "GPUFL_ARCHIVER_ACCESS_KEY"),
                require(args, "archiver-secret-key", "GPUFL_ARCHIVER_SECRET_KEY"),
                resolve(args, "archiver-prefix",     "GPUFL_ARCHIVER_PREFIX", null),
                Boolean.parseBoolean(resolve(args, "archiver-delete", "GPUFL_ARCHIVER_DELETE", "false")));
        }

        LogSourceConfig source = folder != null
            ? new LogSourceConfig(folder, prefix, logTypes) : null;
        return new AgentConfig(source, null, publisher, archiver);
    }

    /**
     * Resolves a config value: checks --flag=value in args first, then the env var, then the default.
     */
    static String resolve(String[] args, String flag, String envVar, String defaultValue) {
        String prefix = "--" + flag + "=";
        for (String arg : args) {
            if (arg.startsWith(prefix)) return arg.substring(prefix.length());
        }
        String env = System.getenv(envVar);
        if (env != null && !env.isBlank()) return env;
        return defaultValue;
    }

    /** Like resolve(), but exits with an error if the value is absent. */
    static String require(String[] args, String flag, String envVar) {
        String val = resolve(args, flag, envVar, null);
        if (val == null) {
            System.err.println("❌ --" + flag + " (or env " + envVar + ") is required");
            printUsage();
            System.exit(1);
        }
        return val;
    }

    /** Log file name pattern: {prefix}.{type}.log */
    private static final Pattern LOG_FILE_PATTERN =
        Pattern.compile("^(.+)\\.(device|scope|system)\\.log$");

    /**
     * Scans a folder for gpufl log files and returns one LogSourceConfig
     * per unique prefix found.  For example, if the folder contains
     * {@code session.device.log} and {@code myapp.scope.log}, two configs
     * are returned with prefixes "session" and "myapp".
     */
    static List<LogSourceConfig> discoverSources(File folder) {
        List<LogSourceConfig> result = new ArrayList<>();
        if (!folder.isDirectory()) return result;

        Set<String> prefixes = new LinkedHashSet<>();
        File[] files = folder.listFiles();
        if (files == null) return result;

        for (File f : files) {
            Matcher m = LOG_FILE_PATTERN.matcher(f.getName());
            if (m.matches()) prefixes.add(m.group(1));
        }

        for (String prefix : prefixes) {
            System.out.println("[agent] Discovered prefix \"" + prefix + "\" in " + folder);
            result.add(new LogSourceConfig(folder.getAbsolutePath(), prefix, null));
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
              --url=<url>                  Endpoint URL             [GPUFL_HTTP_URL]
              --token=<token>              Bearer auth token        [GPUFL_HTTP_TOKEN]
              --timeout=<seconds>          Request timeout          [GPUFL_HTTP_TIMEOUT_SEC]  default: 10

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
              gpufl-agent --folder=/var/log/gpuflight --type=http --url=http://collector:8080/api/v1/events/

              # Env vars (systemd / Docker)
              GPUFL_SOURCE_FOLDER=/var/log GPUFL_PUBLISHER_TYPE=http GPUFL_HTTP_URL=http://... gpufl-agent

              # JSON config file
              gpufl-agent --config=/etc/gpuflight/agent.json

              # No args → uses bundled local.json (development only)
              gpufl-agent
            """);
    }
}
