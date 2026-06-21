package com.gpuflight.agent;

import com.gpuflight.agent.config.ConfigLoader;
import com.gpuflight.agent.config.HttpConfig;
import com.gpuflight.agent.config.KafkaConfig;
import com.gpuflight.agent.model.AgentConfig;
import com.gpuflight.agent.model.ArchiverConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MainTest {

    // ---- resolve() ----

    @Test
    void resolve_returnsFlagValue() {
        String[] args = {"--host=http://example.com", "--token=abc"};
        assertEquals("http://example.com", ConfigLoader.resolve(args, "host", "GPUFL_HTTP_HOST", null, Collections.emptyMap()));
    }

    @Test
    void resolve_returnsSecondFlagValue() {
        String[] args = {"--host=http://example.com", "--token=abc"};
        assertEquals("abc", ConfigLoader.resolve(args, "token", "GPUFL_HTTP_TOKEN", null, Collections.emptyMap()));
    }

    @Test
    void resolve_returnsDefaultWhenAbsent() {
        String[] args = {};
        assertEquals("default-val", ConfigLoader.resolve(args, "missing-flag", "GPUFL_MISSING_VAR_XYZ123", "default-val", Collections.emptyMap()));
    }

    @Test
    void resolve_returnsNullDefaultWhenAbsent() {
        String[] args = {};
        assertNull(ConfigLoader.resolve(args, "missing-flag", "GPUFL_MISSING_VAR_XYZ123", null, Collections.emptyMap()));
    }

    @Test
    void resolve_flagTakesPrecedenceOverDefault() {
        String[] args = {"--folder=/from-cli"};
        assertEquals("/from-cli", ConfigLoader.resolve(args, "folder", "GPUFL_SOURCE_FOLDER_NOTSET_XYZ", "/default", Collections.emptyMap()));
    }

    @Test
    void resolve_partialMatchDoesNotReturn() {
        // --folderExtra should not match --folder=
        String[] args = {"--folderExtra=/wrong"};
        assertNull(ConfigLoader.resolve(args, "folder", "GPUFL_MISSING_XYZ123", null, Collections.emptyMap()));
    }

    // ---- env var support ----

    @Test
    void resolve_returnsEnvVarWhenFlagAbsent() {
        String[] args = {};
        Map<String, String> env = Map.of("GPUFL_HTTP_HOST", "http://env-url.com");
        assertEquals("http://env-url.com", ConfigLoader.resolve(args, "host", "GPUFL_HTTP_HOST", null, env));
    }

    @Test
    void resolve_flagTakesPrecedenceOverEnvVar() {
        String[] args = {"--host=http://cli-url.com"};
        Map<String, String> env = Map.of("GPUFL_HTTP_HOST", "http://env-url.com");
        assertEquals("http://cli-url.com", ConfigLoader.resolve(args, "host", "GPUFL_HTTP_HOST", null, env));
    }

    // ---- log type parsing ----

    @Test
    void parseLogTypes_trimsAndDropsEmptyValues() {
        assertEquals(List.of("system", "device"), ConfigLoader.parseLogTypes(" system, ,device,"));
    }

    @Test
    void parseLogTypes_returnsNullForMissingOrEmptyValues() {
        assertNull(ConfigLoader.parseLogTypes(null));
        assertNull(ConfigLoader.parseLogTypes(" , "));
    }

    @Test
    void logTypesOrDefault_usesExplicitFolderLogTypes() {
        String[] args = {"--folders=/logs", "--log-types=system"};
        assertEquals(List.of("system"), ConfigLoader.logTypesOrDefault(args, Collections.emptyMap()));
    }

    @Test
    void logTypesOrDefault_usesDefaultWhenAbsent() {
        assertEquals(List.of("device", "scope", "system", "sass"),
            ConfigLoader.logTypesOrDefault(new String[]{}, Collections.emptyMap()));
    }

    // ---- require() ----

    @Test
    void require_returnsValueWhenPresent() {
        String[] args = {"--folder=/tmp/logs"};
        assertEquals("/tmp/logs", ConfigLoader.require(args, "folder", "GPUFL_SOURCE_FOLDER", Collections.emptyMap()));
    }

    // ---- parseConfigArg() ----

    @Test
    void parseConfigArg_returnsPath() {
        String[] args = {"--config=/etc/agent.json", "--other=val"};
        assertEquals("/etc/agent.json", ConfigLoader.parseConfigArg(args));
    }

    @Test
    void parseConfigArg_returnsNullWhenAbsent() {
        String[] args = {"--folder=/tmp"};
        assertNull(ConfigLoader.parseConfigArg(args));
    }

    @Test
    void parseConfigArg_emptyArgs() {
        assertNull(ConfigLoader.parseConfigArg(new String[]{}));
    }

    // ---- buildArchiveKey() ----

    @Test
    void buildArchiveKey_concatenatesPrefixAndFilename() {
        Path path = Path.of("/var/log/gpuflight/device.1.log");
        assertEquals("raw-events/device.1.log", ConfigLoader.buildArchiveKey("raw-events/", path));
    }

    @Test
    void buildArchiveKey_emptyPrefix() {
        Path path = Path.of("/some/dir/file.log");
        assertEquals("file.log", ConfigLoader.buildArchiveKey("", path));
    }

    @Test
    void buildArchiveKey_nestedPath() {
        Path path = Path.of("/a/b/c/my.scope.2.log");
        assertEquals("prefix/my.scope.2.log", ConfigLoader.buildArchiveKey("prefix/", path));
    }

    // ---- loadFromArgs() — no flags → classpath fallback ----

    @Test
    void loadFromArgs_noArgsUsesClasspathConfig() {
        boolean hasGpuflEnv = System.getenv().keySet().stream().anyMatch(k -> k.startsWith("GPUFL_"));
        if (!hasGpuflEnv) {
            AgentConfig config = ConfigLoader.loadFromArgs(new String[]{});
            assertNotNull(config);
            assertNotNull(config.source());
            assertEquals(".", config.source().folder());
            assertNotNull(config.publisher());
            assertNull(config.archiver());
        }
    }

    // ---- loadFromArgs() — HTTP publisher ----

    @Test
    void loadFromArgs_httpPublisher_basic() {
        String[] args = {
            "--folder=/var/log", "--type=http",
            "--host=http://localhost:8080"
        };
        AgentConfig config = ConfigLoader.loadFromArgs(args, Collections.emptyMap());
        assertNotNull(config);
        assertEquals("/var/log", config.source().folder());
        assertInstanceOf(HttpConfig.class, config.publisher());
        HttpConfig http = (HttpConfig) config.publisher();
        assertEquals("http://localhost:8080", http.hostUrl());
        assertEquals("v1", http.apiVersion());        // default apiVersion
        assertEquals("http://localhost:8080/api/v1/events/init", http.endpointFor("init"));
        assertNull(http.authToken());
        assertEquals(10L, http.timeoutSeconds()); // default timeout
        assertNull(config.archiver());
    }

    @Test
    void loadFromArgs_httpPublisher_withToken() {
        String[] args = {
            "--folder=/logs", "--type=http",
            "--host=http://collector:8080", "--token=tok123"
        };
        AgentConfig config = ConfigLoader.loadFromArgs(args, Collections.emptyMap());
        HttpConfig http = (HttpConfig) config.publisher();
        assertEquals("tok123", http.authToken());
    }

    @Test
    void loadFromArgs_httpPublisher_customTimeout() {
        String[] args = {
            "--folder=/logs", "--type=http",
            "--host=http://collector:8080", "--timeout=30"
        };
        AgentConfig config = ConfigLoader.loadFromArgs(args, Collections.emptyMap());
        HttpConfig http = (HttpConfig) config.publisher();
        assertEquals(30L, http.timeoutSeconds());
    }

    // ---- loadFromArgs() — Kafka publisher ----

    @Test
    void loadFromArgs_kafkaPublisher_withOptions() {
        String[] args = {
            "--folder=/var/log", "--type=kafka",
            "--brokers=broker1:9092,broker2:9092",
            "--topic-prefix=my-topic", "--compression=lz4"
        };
        AgentConfig config = ConfigLoader.loadFromArgs(args, Collections.emptyMap());
        assertInstanceOf(KafkaConfig.class, config.publisher());
        KafkaConfig kafka = (KafkaConfig) config.publisher();
        assertEquals("broker1:9092,broker2:9092", kafka.bootstrapServers());
        assertEquals("my-topic", kafka.topicPrefix());
        assertEquals("lz4", kafka.compression());
    }

    @Test
    void loadFromArgs_kafkaPublisher_defaultsApplied() {
        String[] args = {
            "--folder=/var/log", "--type=kafka",
            "--brokers=localhost:9092"
        };
        AgentConfig config = ConfigLoader.loadFromArgs(args, Collections.emptyMap());
        KafkaConfig kafka = (KafkaConfig) config.publisher();
        // KafkaConfig compact constructor replaces null with defaults
        assertEquals("gpu-trace", kafka.topicPrefix());
        assertEquals("snappy", kafka.compression());
    }

    // ---- loadFromArgs() — with Archiver ----

    @Test
    void loadFromArgs_withArchiver_defaults() {
        String[] args = {
            "--folder=/logs", "--type=http", "--host=http://localhost",
            "--archiver-endpoint=http://minio:9000",
            "--archiver-bucket=my-bucket",
            "--archiver-access-key=AKIAKEY",
            "--archiver-secret-key=SECRET"
        };
        AgentConfig config = ConfigLoader.loadFromArgs(args, Collections.emptyMap());
        assertNotNull(config.archiver());
        assertEquals("http://minio:9000", config.archiver().endpoint());
        assertEquals("my-bucket", config.archiver().bucket());
        assertEquals("AKIAKEY", config.archiver().accessKey());
        assertEquals("SECRET", config.archiver().secretKey());
        assertEquals("nyc3", config.archiver().region());         // default
        assertEquals("raw-events/", config.archiver().prefix());  // default
        assertFalse(config.archiver().deleteAfterUpload());        // default
    }

    @Test
    void loadFromArgs_withArchiver_explicitOptions() {
        String[] args = {
            "--folder=/logs", "--type=http", "--host=http://localhost",
            "--archiver-endpoint=http://s3.example.com",
            "--archiver-bucket=bucket",
            "--archiver-region=us-east-1",
            "--archiver-access-key=KEY",
            "--archiver-secret-key=SKEY",
            "--archiver-prefix=logs/",
            "--archiver-delete=true"
        };
        AgentConfig config = ConfigLoader.loadFromArgs(args, Collections.emptyMap());
        ArchiverConfig archiver = config.archiver();
        assertNotNull(archiver);
        assertEquals("us-east-1", archiver.region());
        assertEquals("logs/", archiver.prefix());
        assertTrue(archiver.deleteAfterUpload());
    }

    @Test
    void loadFromArgs_noArchiver_whenEndpointAbsent() {
        String[] args = {
            "--folder=/logs", "--type=http", "--host=http://localhost"
        };
        AgentConfig config = ConfigLoader.loadFromArgs(args, Collections.emptyMap());
        assertNull(config.archiver());
    }

    // ---- parseConfigArg with --config path ----

    @Test
    void parseConfigArg_extractsPathFromConfigFlag(@TempDir Path tempDir) throws IOException {
        String json = """
            {
              "source": { "folder": "/tmp/test", "filePrefix": "testapp" },
              "publisher": { "type": "http", "hostUrl": "http://localhost", "authToken": null }
            }
            """;
        File configFile = tempDir.resolve("agent.json").toFile();
        Files.writeString(configFile.toPath(), json);

        // Invoke via parseConfigArg + direct public method chain
        String[] args = {"--config=" + configFile.getAbsolutePath()};
        String configPath = ConfigLoader.parseConfigArg(args);
        assertNotNull(configPath);
        assertEquals(configFile.getAbsolutePath(), configPath);
    }

    // ---- loadExternalConfig (private) via reflection ----

    @Test
    void loadExternalConfig_loadsValidJsonFile(@TempDir Path tempDir) throws Exception {
        String json = """
            {
              "source": { "folder": "/tmp/ext", "filePrefix": "extapp" },
              "publisher": { "type": "http", "hostUrl": "http://localhost:9090", "authToken": "tok" }
            }
            """;
        File configFile = tempDir.resolve("ext.json").toFile();
        Files.writeString(configFile.toPath(), json);

        Method m = ConfigLoader.class.getDeclaredMethod("loadExternalConfig", String.class);
        m.setAccessible(true);
        AgentConfig config = (AgentConfig) m.invoke(null, configFile.getAbsolutePath());

        assertNotNull(config);
        assertEquals("/tmp/ext", config.source().folder());
        assertInstanceOf(HttpConfig.class, config.publisher());
    }

    @Test
    void loadExternalConfig_withKafkaPublisher(@TempDir Path tempDir) throws Exception {
        String json = """
            {
              "source": { "folder": "/logs", "filePrefix": "prod" },
              "publisher": { "type": "kafka", "bootstrapServers": "b1:9092", "topicPrefix": "t1" }
            }
            """;
        File configFile = tempDir.resolve("kafka.json").toFile();
        Files.writeString(configFile.toPath(), json);

        Method m = ConfigLoader.class.getDeclaredMethod("loadExternalConfig", String.class);
        m.setAccessible(true);
        AgentConfig config = (AgentConfig) m.invoke(null, configFile.getAbsolutePath());

        assertNotNull(config);
        assertInstanceOf(KafkaConfig.class, config.publisher());
        KafkaConfig kafka = (KafkaConfig) config.publisher();
        assertEquals("b1:9092", kafka.bootstrapServers());
    }

    @Test
    void loadExternalConfig_withArchiver(@TempDir Path tempDir) throws Exception {
        String json = """
            {
              "source": { "folder": "/logs", "filePrefix": "app" },
              "publisher": { "type": "http", "hostUrl": "http://localhost" },
              "archiver": {
                "endpoint": "http://minio:9000",
                "bucket": "logs",
                "accessKey": "K",
                "secretKey": "S"
              }
            }
            """;
        File configFile = tempDir.resolve("arch.json").toFile();
        Files.writeString(configFile.toPath(), json);

        Method m = ConfigLoader.class.getDeclaredMethod("loadExternalConfig", String.class);
        m.setAccessible(true);
        AgentConfig config = (AgentConfig) m.invoke(null, configFile.getAbsolutePath());

        assertNotNull(config.archiver());
        assertEquals("http://minio:9000", config.archiver().endpoint());
        assertEquals("nyc3", config.archiver().region()); // default applied
    }

    // ---- loadExternalConfig — sources array ----

    @Test
    void loadExternalConfig_sourcesArray(@TempDir Path tempDir) throws Exception {
        String json = """
            {
              "sources": [
                { "folder": "/logs/app1", "filePrefix": "app1" },
                { "folder": "/logs/app2", "filePrefix": "app2" }
              ],
              "publisher": { "type": "http", "hostUrl": "http://localhost" }
            }
            """;
        File configFile = tempDir.resolve("multi.json").toFile();
        Files.writeString(configFile.toPath(), json);

        Method m = ConfigLoader.class.getDeclaredMethod("loadExternalConfig", String.class);
        m.setAccessible(true);
        AgentConfig config = (AgentConfig) m.invoke(null, configFile.getAbsolutePath());

        assertNotNull(config);
        assertNull(config.source());
        assertNotNull(config.sources());
        assertEquals(2, config.sources().size());
        assertEquals("/logs/app1", config.sources().get(0).folder());
        assertEquals("/logs/app2", config.sources().get(1).folder());
    }

    @Test
    void loadExternalConfig_sourceAndSourcesMerged(@TempDir Path tempDir) throws Exception {
        String json = """
            {
              "source": { "folder": "/logs/main", "filePrefix": "main" },
              "sources": [
                { "folder": "/logs/extra", "filePrefix": "extra" }
              ],
              "publisher": { "type": "http", "hostUrl": "http://localhost" }
            }
            """;
        File configFile = tempDir.resolve("both.json").toFile();
        Files.writeString(configFile.toPath(), json);

        Method m = ConfigLoader.class.getDeclaredMethod("loadExternalConfig", String.class);
        m.setAccessible(true);
        AgentConfig config = (AgentConfig) m.invoke(null, configFile.getAbsolutePath());

        assertNotNull(config);
        assertNotNull(config.source());
        assertEquals("/logs/main", config.source().folder());
        assertNotNull(config.sources());
        assertEquals(1, config.sources().size());
        assertEquals("/logs/extra", config.sources().get(0).folder());
    }

    // ---- topicPrefix() ----

    @Test
    void topicPrefix_httpConfig_returnsGpuTrace() {
        AgentConfig config = ConfigLoader.loadFromArgs(new String[]{
            "--folder=/logs", "--type=http", "--host=http://localhost"
        });
        assertEquals("gpu-trace", GpuflAgent.topicPrefix(config));
    }

    @Test
    void topicPrefix_kafkaConfig_returnsConfiguredPrefix() {
        AgentConfig config = ConfigLoader.loadFromArgs(new String[]{
            "--folder=/logs", "--type=kafka",
            "--brokers=localhost:9092", "--topic-prefix=my-prefix"
        });
        assertEquals("my-prefix", GpuflAgent.topicPrefix(config));
    }

    // ---- loadClasspathConfig (private) via reflection ----

    @Test
    void loadClasspathConfig_loadsLocalJson() throws Exception {
        Method m = ConfigLoader.class.getDeclaredMethod("loadClasspathConfig", String.class);
        m.setAccessible(true);
        AgentConfig config = (AgentConfig) m.invoke(null, "config/local.json");

        assertNotNull(config);
        assertEquals(".", config.source().folder());
    }

    // ---- exit-when-drained helpers ----

    @Test
    void parseExitWhenDrained_onForEnvOrFlag() {
        assertTrue(ConfigLoader.parseExitWhenDrained(new String[]{}, Map.of("GPUFL_AGENT_EXIT_WHEN_DRAINED", "1")));
        assertTrue(ConfigLoader.parseExitWhenDrained(new String[]{"--exit-when-drained=true"}, Collections.emptyMap()));
    }

    @Test
    void parseExitWhenDrained_offWhenAbsentOrFalsey() {
        assertFalse(ConfigLoader.parseExitWhenDrained(new String[]{}, Collections.emptyMap()));
        assertFalse(ConfigLoader.parseExitWhenDrained(new String[]{}, Map.of("GPUFL_AGENT_EXIT_WHEN_DRAINED", "0")));
        assertFalse(ConfigLoader.parseExitWhenDrained(new String[]{"--exit-when-drained=false"}, Collections.emptyMap()));
    }

    @Test
    void anyActiveSession_trueWhenSessionStillWriting(@TempDir Path dir) throws IOException {
        File folder = dir.toFile();
        Files.createDirectories(new File(new File(folder, "sess-1"), ".tmp").toPath()); // .tmp = active
        assertTrue(GpuflAgent.anyActiveSession(List.of(folder)));
    }

    @Test
    void anyActiveSession_falseWhenDrainedOrEmpty(@TempDir Path dir) throws IOException {
        File folder = dir.toFile();
        File session = new File(folder, "sess-1");
        Files.createDirectories(session.toPath());
        Files.writeString(new File(session, "device.1.log.gz").toPath(), "x"); // finished window, no .tmp
        assertFalse(GpuflAgent.anyActiveSession(List.of(folder)));
        assertFalse(GpuflAgent.anyActiveSession(List.of(new File(folder, "missing")))); // non-existent folder
    }

    // ---- printUsage() — smoke test ----

    @Test
    void printUsage_doesNotThrow() {
        PrintStream original = System.err;
        try {
            System.setErr(new PrintStream(PrintStream.nullOutputStream()));
            ConfigLoader.printUsage();
        } finally {
            System.setErr(original);
        }
    }
}
