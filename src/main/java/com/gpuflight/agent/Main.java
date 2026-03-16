package com.gpuflight.agent;

import com.gpuflight.agent.config.HttpConfig;
import com.gpuflight.agent.config.JsonSettings;
import com.gpuflight.agent.config.KafkaConfig;
import com.gpuflight.agent.config.PublisherConfig;
import com.gpuflight.agent.model.AgentConfig;
import com.gpuflight.agent.model.ArchiverConfig;
import com.gpuflight.agent.model.LogSourceConfig;
import com.gpuflight.agent.publisher.Publisher;
import com.gpuflight.agent.publisher.PublisherFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {
    private static final List<String> LOG_TYPES = List.of("device", "scope", "system");

    public static void main(String[] args) throws Exception {
        String configPath = parseConfigArg(args);
        AgentConfig config = configPath != null
            ? loadExternalConfig(configPath)
            : loadFromArgs(args);

        Publisher publisher = PublisherFactory.create(config.publisher());
        var source = config.source();
        var folder = new File(source.folder());
        var cursorMgr = new CursorManager(new File("./cursor.json"));
        var consumedFilesQueue = new LinkedBlockingQueue<Path>();

        var executor = Executors.newVirtualThreadPerTaskExecutor();

        for (String type : LOG_TYPES) {
            executor.submit(() -> {
                var tailer = new LogTailer(folder, source.filePrefix(), type, cursorMgr, consumedFilesQueue);
                tailer.tail(publisher);
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
        boolean hasAnyConfig = Arrays.stream(args).anyMatch(a -> a.startsWith("--"))
            || System.getenv().keySet().stream().anyMatch(k -> k.startsWith("GPUFL_"));

        if (!hasAnyConfig) {
            System.out.println("No flags or GPUFL_* env vars found — using bundled local.json");
            return loadClasspathConfig("config/local.json");
        }

        String folder = require(args, "folder", "GPUFL_SOURCE_FOLDER");
        String prefix = resolve(args, "prefix", "GPUFL_SOURCE_PREFIX", "gpufl");
        String type   = require(args, "type",   "GPUFL_PUBLISHER_TYPE");

        PublisherConfig publisher = switch (type.toLowerCase()) {
            case "http"  -> new HttpConfig(
                require(args, "url",     "GPUFL_HTTP_URL"),
                resolve(args, "token",   "GPUFL_HTTP_TOKEN", null),
                Long.parseLong(resolve(args, "timeout", "GPUFL_HTTP_TIMEOUT_SEC", "10")));
            case "kafka" -> new KafkaConfig(
                require(args, "brokers",      "GPUFL_KAFKA_BROKERS"),
                resolve(args, "topic-prefix", "GPUFL_KAFKA_TOPIC_PREFIX", null),
                resolve(args, "compression",  "GPUFL_KAFKA_COMPRESSION", null));
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

        return new AgentConfig(new LogSourceConfig(folder, prefix), publisher, archiver);
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

    static String parseConfigArg(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--config=")) return arg.substring("--config=".length());
        }
        return null;
    }

    static String buildArchiveKey(String prefix, Path path) {
        return prefix + path.toFile().getName();
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

            Source (required):
              --folder=<path>              Log folder path          [GPUFL_SOURCE_FOLDER]
              --prefix=<name>              Log file prefix          [GPUFL_SOURCE_PREFIX]  default: gpufl

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
