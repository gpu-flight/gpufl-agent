package com.gpuflight.agent;

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

import static org.junit.jupiter.api.Assertions.*;

class MainTest {

    // ---- resolve() ----

    @Test
    void resolve_returnsFlagValue() {
        String[] args = {"--url=http://example.com", "--token=abc"};
        assertEquals("http://example.com", Main.resolve(args, "url", "GPUFL_HTTP_URL", null));
    }

    @Test
    void resolve_returnsSecondFlagValue() {
        String[] args = {"--url=http://example.com", "--token=abc"};
        assertEquals("abc", Main.resolve(args, "token", "GPUFL_HTTP_TOKEN", null));
    }

    @Test
    void resolve_returnsDefaultWhenAbsent() {
        String[] args = {};
        assertEquals("default-val", Main.resolve(args, "missing-flag", "GPUFL_MISSING_VAR_XYZ123", "default-val"));
    }

    @Test
    void resolve_returnsNullDefaultWhenAbsent() {
        String[] args = {};
        assertNull(Main.resolve(args, "missing-flag", "GPUFL_MISSING_VAR_XYZ123", null));
    }

    @Test
    void resolve_flagTakesPrecedenceOverDefault() {
        String[] args = {"--folder=/from-cli"};
        assertEquals("/from-cli", Main.resolve(args, "folder", "GPUFL_SOURCE_FOLDER_NOTSET_XYZ", "/default"));
    }

    @Test
    void resolve_partialMatchDoesNotReturn() {
        // --folderExtra should not match --folder=
        String[] args = {"--folderExtra=/wrong"};
        assertNull(Main.resolve(args, "folder", "GPUFL_MISSING_XYZ123", null));
    }

    // ---- require() ----

    @Test
    void require_returnsValueWhenPresent() {
        String[] args = {"--folder=/tmp/logs"};
        assertEquals("/tmp/logs", Main.require(args, "folder", "GPUFL_SOURCE_FOLDER"));
    }

    // ---- parseConfigArg() ----

    @Test
    void parseConfigArg_returnsPath() {
        String[] args = {"--config=/etc/agent.json", "--other=val"};
        assertEquals("/etc/agent.json", Main.parseConfigArg(args));
    }

    @Test
    void parseConfigArg_returnsNullWhenAbsent() {
        String[] args = {"--folder=/tmp"};
        assertNull(Main.parseConfigArg(args));
    }

    @Test
    void parseConfigArg_emptyArgs() {
        assertNull(Main.parseConfigArg(new String[]{}));
    }

    // ---- buildArchiveKey() ----

    @Test
    void buildArchiveKey_concatenatesPrefixAndFilename() {
        Path path = Path.of("/var/log/gpuflight/device.1.log");
        assertEquals("raw-events/device.1.log", Main.buildArchiveKey("raw-events/", path));
    }

    @Test
    void buildArchiveKey_emptyPrefix() {
        Path path = Path.of("/some/dir/file.log");
        assertEquals("file.log", Main.buildArchiveKey("", path));
    }

    @Test
    void buildArchiveKey_nestedPath() {
        Path path = Path.of("/a/b/c/my.scope.2.log");
        assertEquals("prefix/my.scope.2.log", Main.buildArchiveKey("prefix/", path));
    }

    // ---- loadFromArgs() — no flags → classpath fallback ----

    @Test
    void loadFromArgs_noArgsUsesClasspathConfig() {
        boolean hasGpuflEnv = System.getenv().keySet().stream().anyMatch(k -> k.startsWith("GPUFL_"));
        if (!hasGpuflEnv) {
            AgentConfig config = Main.loadFromArgs(new String[]{});
            assertNotNull(config);
            assertNotNull(config.source());
            // local.json has filePrefix "sass_divergence"
            assertEquals("session", config.source().filePrefix());
            assertNotNull(config.publisher());
            assertNull(config.archiver());
        }
    }

    // ---- loadFromArgs() — HTTP publisher ----

    @Test
    void loadFromArgs_httpPublisher_basic() {
        String[] args = {
            "--folder=/var/log", "--type=http",
            "--url=http://localhost:8080/api/v1/events/"
        };
        AgentConfig config = Main.loadFromArgs(args);
        assertNotNull(config);
        assertEquals("/var/log", config.source().folder());
        assertEquals("gpufl", config.source().filePrefix()); // default prefix
        assertInstanceOf(HttpConfig.class, config.publisher());
        HttpConfig http = (HttpConfig) config.publisher();
        assertEquals("http://localhost:8080/api/v1/events/", http.endpointUrl());
        assertNull(http.authToken());
        assertEquals(10L, http.timeoutSeconds()); // default timeout
        assertNull(config.archiver());
    }

    @Test
    void loadFromArgs_httpPublisher_withToken() {
        String[] args = {
            "--folder=/logs", "--type=http",
            "--url=http://collector:8080/", "--token=tok123"
        };
        AgentConfig config = Main.loadFromArgs(args);
        HttpConfig http = (HttpConfig) config.publisher();
        assertEquals("tok123", http.authToken());
    }

    @Test
    void loadFromArgs_httpPublisher_customTimeout() {
        String[] args = {
            "--folder=/logs", "--type=http",
            "--url=http://collector:8080/", "--timeout=30"
        };
        AgentConfig config = Main.loadFromArgs(args);
        HttpConfig http = (HttpConfig) config.publisher();
        assertEquals(30L, http.timeoutSeconds());
    }

    @Test
    void loadFromArgs_httpPublisher_customPrefix() {
        String[] args = {
            "--folder=/logs", "--prefix=myapp", "--type=http",
            "--url=http://localhost/"
        };
        AgentConfig config = Main.loadFromArgs(args);
        assertEquals("myapp", config.source().filePrefix());
    }

    // ---- loadFromArgs() — Kafka publisher ----

    @Test
    void loadFromArgs_kafkaPublisher_withOptions() {
        String[] args = {
            "--folder=/var/log", "--type=kafka",
            "--brokers=broker1:9092,broker2:9092",
            "--topic-prefix=my-topic", "--compression=lz4"
        };
        AgentConfig config = Main.loadFromArgs(args);
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
        AgentConfig config = Main.loadFromArgs(args);
        KafkaConfig kafka = (KafkaConfig) config.publisher();
        // KafkaConfig compact constructor replaces null with defaults
        assertEquals("gpu-trace", kafka.topicPrefix());
        assertEquals("snappy", kafka.compression());
    }

    // ---- loadFromArgs() — with Archiver ----

    @Test
    void loadFromArgs_withArchiver_defaults() {
        String[] args = {
            "--folder=/logs", "--type=http", "--url=http://localhost/",
            "--archiver-endpoint=http://minio:9000",
            "--archiver-bucket=my-bucket",
            "--archiver-access-key=AKIAKEY",
            "--archiver-secret-key=SECRET"
        };
        AgentConfig config = Main.loadFromArgs(args);
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
            "--folder=/logs", "--type=http", "--url=http://localhost/",
            "--archiver-endpoint=http://s3.example.com",
            "--archiver-bucket=bucket",
            "--archiver-region=us-east-1",
            "--archiver-access-key=KEY",
            "--archiver-secret-key=SKEY",
            "--archiver-prefix=logs/",
            "--archiver-delete=true"
        };
        AgentConfig config = Main.loadFromArgs(args);
        ArchiverConfig archiver = config.archiver();
        assertNotNull(archiver);
        assertEquals("us-east-1", archiver.region());
        assertEquals("logs/", archiver.prefix());
        assertTrue(archiver.deleteAfterUpload());
    }

    @Test
    void loadFromArgs_noArchiver_whenEndpointAbsent() {
        String[] args = {
            "--folder=/logs", "--type=http", "--url=http://localhost/"
        };
        AgentConfig config = Main.loadFromArgs(args);
        assertNull(config.archiver());
    }

    // ---- parseConfigArg with --config path ----

    @Test
    void parseConfigArg_extractsPathFromConfigFlag(@TempDir Path tempDir) throws IOException {
        String json = """
            {
              "source": { "folder": "/tmp/test", "filePrefix": "testapp" },
              "publisher": { "type": "http", "endpointUrl": "http://localhost/", "authToken": null }
            }
            """;
        File configFile = tempDir.resolve("agent.json").toFile();
        Files.writeString(configFile.toPath(), json);

        // Invoke via parseConfigArg + direct public method chain
        String[] args = {"--config=" + configFile.getAbsolutePath()};
        String configPath = Main.parseConfigArg(args);
        assertNotNull(configPath);
        assertEquals(configFile.getAbsolutePath(), configPath);
    }

    // ---- loadExternalConfig (private) via reflection ----

    @Test
    void loadExternalConfig_loadsValidJsonFile(@TempDir Path tempDir) throws Exception {
        String json = """
            {
              "source": { "folder": "/tmp/ext", "filePrefix": "extapp" },
              "publisher": { "type": "http", "endpointUrl": "http://localhost:9090/", "authToken": "tok" }
            }
            """;
        File configFile = tempDir.resolve("ext.json").toFile();
        Files.writeString(configFile.toPath(), json);

        Method m = Main.class.getDeclaredMethod("loadExternalConfig", String.class);
        m.setAccessible(true);
        AgentConfig config = (AgentConfig) m.invoke(null, configFile.getAbsolutePath());

        assertNotNull(config);
        assertEquals("/tmp/ext", config.source().folder());
        assertEquals("extapp", config.source().filePrefix());
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

        Method m = Main.class.getDeclaredMethod("loadExternalConfig", String.class);
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
              "publisher": { "type": "http", "endpointUrl": "http://localhost/" },
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

        Method m = Main.class.getDeclaredMethod("loadExternalConfig", String.class);
        m.setAccessible(true);
        AgentConfig config = (AgentConfig) m.invoke(null, configFile.getAbsolutePath());

        assertNotNull(config.archiver());
        assertEquals("http://minio:9000", config.archiver().endpoint());
        assertEquals("nyc3", config.archiver().region()); // default applied
    }

    // ---- topicPrefix() ----

    @Test
    void topicPrefix_httpConfig_returnsGpuTrace() {
        AgentConfig config = Main.loadFromArgs(new String[]{
            "--folder=/logs", "--type=http", "--url=http://localhost/"
        });
        assertEquals("gpu-trace", Main.topicPrefix(config));
    }

    @Test
    void topicPrefix_kafkaConfig_returnsConfiguredPrefix() {
        AgentConfig config = Main.loadFromArgs(new String[]{
            "--folder=/logs", "--type=kafka",
            "--brokers=localhost:9092", "--topic-prefix=my-prefix"
        });
        assertEquals("my-prefix", Main.topicPrefix(config));
    }

    // ---- loadClasspathConfig (private) via reflection ----

    @Test
    void loadClasspathConfig_loadsLocalJson() throws Exception {
        Method m = Main.class.getDeclaredMethod("loadClasspathConfig", String.class);
        m.setAccessible(true);
        AgentConfig config = (AgentConfig) m.invoke(null, "config/local.json");

        assertNotNull(config);
        assertEquals("session", config.source().filePrefix());
    }

    // ---- printUsage() — smoke test ----

    @Test
    void printUsage_doesNotThrow() {
        PrintStream original = System.err;
        try {
            System.setErr(new PrintStream(PrintStream.nullOutputStream()));
            Main.printUsage();
        } finally {
            System.setErr(original);
        }
    }
}
