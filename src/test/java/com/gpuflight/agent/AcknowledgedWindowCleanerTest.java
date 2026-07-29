package com.gpuflight.agent;

import com.gpuflight.agent.config.JsonSettings;
import com.gpuflight.agent.model.WindowMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class AcknowledgedWindowCleanerTest {

    @Test
    void ackDeletesIdentityPayloadButRetainsSequenceTombstone(
            @TempDir Path root) throws Exception {
        Path session = Files.createDirectories(root.resolve("session-a"));
        Path payload = session.resolve("device.1.log.gz");
        Files.write(payload, new byte[] {1, 2, 3});
        Path metadata = writeMetadata(
                session, "session-a", "device", 1, payload.getFileName().toString());

        assertTrue(AcknowledgedWindowCleaner.deleteIfIdentityAware(payload));
        assertFalse(Files.exists(payload));
        assertTrue(Files.isRegularFile(metadata));
    }

    @Test
    void legacyPayloadWithoutMetadataIsRetained(@TempDir Path root)
            throws Exception {
        Path session = Files.createDirectories(root.resolve("legacy"));
        Path payload = session.resolve("device.1.log.gz");
        Files.write(payload, new byte[] {1});

        assertFalse(AcknowledgedWindowCleaner.deleteIfIdentityAware(payload));
        assertTrue(Files.isRegularFile(payload));
    }

    @Test
    void plainWindowIsNotMistakenForIdentityAwareGzip(
            @TempDir Path root) throws Exception {
        Path session = Files.createDirectories(root.resolve("session-a"));
        Path payload = session.resolve("device.1.log");
        Files.write(payload, new byte[] {1});
        writeMetadata(
                session, "session-a", "device", 1,
                payload.getFileName().toString());

        assertFalse(AcknowledgedWindowCleaner.deleteIfIdentityAware(payload));
        assertTrue(Files.isRegularFile(payload));
    }

    @Test
    void mismatchedMetadataCannotAuthorizeDeletion(@TempDir Path root)
            throws Exception {
        Path session = Files.createDirectories(root.resolve("session-a"));
        Path payload = session.resolve("device.1.log.gz");
        Files.write(payload, new byte[] {1});
        writeMetadata(
                session, "different-session", "device", 1,
                payload.getFileName().toString());

        assertFalse(AcknowledgedWindowCleaner.deleteIfIdentityAware(payload));
        assertTrue(Files.isRegularFile(payload));
    }

    private static Path writeMetadata(
            Path sessionDir, String sessionId, String channel,
            long sequence, String payloadFile) throws Exception {
        WindowMetadata metadata = new WindowMetadata(
                1, "transport_window",
                "11111111-2222-4333-8444-555555555555",
                sessionId, channel, sequence, 10, 20, 30,
                payloadFile, 3, 123);
        Path path = sessionDir.resolve(
                ".gpufl-window." + channel + "." + sequence + ".json");
        Files.writeString(path, JsonSettings.MAPPER.writeValueAsString(metadata));
        return path;
    }
}
