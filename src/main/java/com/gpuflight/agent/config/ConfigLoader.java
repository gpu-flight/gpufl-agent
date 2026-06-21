package com.gpuflight.agent.config;

import com.gpuflight.agent.model.AgentConfig;
import com.gpuflight.agent.model.ArchiverConfig;
import com.gpuflight.agent.model.LogSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(ConfigLoader.class);

    public static final List<String> DEFAULT_LOG_TYPES = List.of("device", "scope", "system", "sass");

    public static AgentConfig load(String[] args) {
        String configPath = parseConfigArg(args);
        if (configPath != null) {
            return loadExternalConfig(configPath);
        }
        return loadFromArgs(args);
    }

    public static String parseConfigArg(String[] args) {
        for (String arg : args) {
            if (arg.startsWith("--config=")) return arg.substring("--config=".length());
        }
        return null;
    }

    public static AgentConfig loadFromArgs(String[] args) {
        return loadFromArgs(args, System.getenv());
    }

    public static AgentConfig loadFromArgs(String[] args, Map<String, String> env) {
        boolean hasAnyConfig = Arrays.stream(args).anyMatch(a -> a.startsWith("--"))
                || env.keySet().stream().anyMatch(k -> k.startsWith("GPUFL_"));

        if (!hasAnyConfig) {
            log.info("No flags or GPUFL_* env vars found - using bundled local.json");
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
        String type = require(args, "type", "GPUFL_PUBLISHER_TYPE", env);

        if (resolve(args, "url", "GPUFL_HTTP_URL", null, env) != null) {
            System.err.println("ERROR: --url / GPUFL_HTTP_URL is no longer supported.");
            System.err.println("   Use --host=<scheme://host[:port]>      (or env GPUFL_HTTP_HOST)");
            System.err.println("       --api-version=<v1|v2|...>          (or env GPUFL_HTTP_API_VERSION, default: v1)");
            System.err.println("   The /api/{version}/events/ path is now built automatically.");
            System.exit(1);
        }

        PublisherConfig publisher = switch (type.toLowerCase()) {
            case "http" -> new HttpConfig(
                    require(args, "host", "GPUFL_HTTP_HOST", env),
                    resolve(args, "api-version", "GPUFL_HTTP_API_VERSION", HttpConfig.DEFAULT_API_VERSION, env),
                    resolve(args, "token", "GPUFL_HTTP_TOKEN", null, env),
                    Long.parseLong(resolve(args, "timeout", "GPUFL_HTTP_TIMEOUT_SEC", "10", env)),
                    resolve(args, "upload-mode", "GPUFL_AGENT_UPLOAD_MODE", HttpConfig.DEFAULT_UPLOAD_MODE, env),
                    Integer.parseInt(resolve(args, "stream-max-lines", "GPUFL_AGENT_STREAM_MAX_LINES", "0", env)),
                    Long.parseLong(resolve(args, "stream-max-bytes", "GPUFL_AGENT_STREAM_MAX_BYTES", "0", env)));
            case "kafka" -> new KafkaConfig(
                    require(args, "brokers", "GPUFL_KAFKA_BROKERS", env),
                    resolve(args, "topic-prefix", "GPUFL_KAFKA_TOPIC_PREFIX", null, env),
                    resolve(args, "compression", "GPUFL_KAFKA_COMPRESSION", null, env),
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
                    require(args, "archiver-bucket", "GPUFL_ARCHIVER_BUCKET", env),
                    resolve(args, "archiver-region", "GPUFL_ARCHIVER_REGION", null, env),
                    require(args, "archiver-access-key", "GPUFL_ARCHIVER_ACCESS_KEY", env),
                    require(args, "archiver-secret-key", "GPUFL_ARCHIVER_SECRET_KEY", env),
                    resolve(args, "archiver-prefix", "GPUFL_ARCHIVER_PREFIX", null, env),
                    Boolean.parseBoolean(resolve(args, "archiver-delete", "GPUFL_ARCHIVER_DELETE", "false", env)));
        }

        LogSourceConfig source = folder != null
                ? new LogSourceConfig(folder, logTypes) : null;
        return new AgentConfig(source, null, publisher, archiver);
    }

    public static List<String> parseLogTypes(String raw) {
        if (raw == null) return null;
        List<String> parsed = Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
        return parsed.isEmpty() ? null : parsed;
    }

    public static List<String> logTypesOrDefault(String[] args, Map<String, String> env) {
        List<String> parsed = parseLogTypes(resolve(args, "log-types", "GPUFL_LOG_TYPES", null, env));
        return parsed == null ? DEFAULT_LOG_TYPES : parsed;
    }

    public static String resolve(String[] args, String flag, String envVar, String defaultValue) {
        return resolve(args, flag, envVar, defaultValue, System.getenv());
    }

    public static String resolve(String[] args, String flag, String envVar, String defaultValue, Map<String, String> env) {
        String prefix = "--" + flag + "=";
        for (String arg : args) {
            if (arg.startsWith(prefix)) return arg.substring(prefix.length());
        }
        String envVal = env.get(envVar);
        if (envVal != null && !envVal.isBlank()) return envVal;
        return defaultValue;
    }

    public static String require(String[] args, String flag, String envVar, Map<String, String> env) {
        String val = resolve(args, flag, envVar, null, env);
        if (val == null) {
            System.err.println("ERROR: --" + flag + " (or env " + envVar + ") is required");
            printUsage();
            System.exit(1);
        }
        return val;
    }

    public static boolean parseExitWhenDrained(String[] args, Map<String, String> env) {
        String v = resolve(args, "exit-when-drained", "GPUFL_AGENT_EXIT_WHEN_DRAINED", null, env);
        return v != null && !v.equalsIgnoreCase("false") && !v.equals("0");
    }

    public static String buildArchiveKey(String prefix, Path path) {
        return prefix + path.toFile().getName();
    }

    private static AgentConfig loadExternalConfig(String path) {
        File file = new File(path);
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
        try (InputStream stream = ConfigLoader.class.getClassLoader().getResourceAsStream(resourcePath)) {
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

    public static void printUsage() {
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
