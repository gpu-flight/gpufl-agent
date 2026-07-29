package com.gpuflight.agent;

import com.gpuflight.agent.config.JsonSettings;
import com.gpuflight.agent.model.WindowMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Deletes a transport payload only after its backend ACK while retaining the
 * immutable metadata sidecar as a sequence tombstone.
 *
 * <p>Legacy windows have no sidecar and are intentionally retained: their
 * HTTP success is not backed by the transport-window registry, so this class
 * never guesses that they are safely replayable.
 */
public final class AcknowledgedWindowCleaner {
    private static final Logger log =
            LoggerFactory.getLogger(AcknowledgedWindowCleaner.class);
    private static final Pattern PAYLOAD = Pattern.compile(
            "^([A-Za-z0-9_-]+)\\.([1-9][0-9]*)\\.log\\.gz$");

    private AcknowledgedWindowCleaner() {}

    public static Path acknowledgementPath(
            Path sessionDir, String channel, long sequence) {
        return sessionDir.resolve(
                ".gpufl-window-ack." + channel + "." + sequence);
    }

    /**
     * Durable local proof that the backend explicitly acknowledged the
     * transport identity. A plain 2xx from an older backend is not sufficient.
     */
    public static boolean recordBackendAcknowledgement(
            Path sessionDir, WindowMetadata metadata) {
        Path marker = acknowledgementPath(
                sessionDir, metadata.channel(), metadata.windowSequence());
        if (Files.isRegularFile(marker)) return true;
        byte[] body = (metadata.windowId() + "\n")
                .getBytes(StandardCharsets.UTF_8);
        try (FileChannel out = FileChannel.open(
                marker, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            ByteBuffer bytes = ByteBuffer.wrap(body);
            while (bytes.hasRemaining()) out.write(bytes);
            out.force(true);
            return true;
        } catch (FileAlreadyExistsException alreadyRecorded) {
            return Files.isRegularFile(marker);
        } catch (IOException failure) {
            log.error("Could not persist backend window acknowledgement {}: {}",
                    marker, failure.getMessage());
            return false;
        }
    }

    public static boolean hasBackendAcknowledgement(
            Path sessionDir, String channel, long sequence) {
        Path metadataPath = sessionDir.resolve(
                ".gpufl-window." + channel + "." + sequence + ".json");
        if (!Files.isRegularFile(metadataPath)) return false;
        try {
            WindowMetadata metadata = JsonSettings.MAPPER.readValue(
                    metadataPath.toFile(), WindowMetadata.class);
            return metadata.channel().equals(channel)
                    && metadata.windowSequence() == sequence
                    && hasBackendAcknowledgement(sessionDir, metadata);
        } catch (RuntimeException malformed) {
            return false;
        }
    }

    private static boolean hasBackendAcknowledgement(
            Path sessionDir, WindowMetadata metadata) {
        Path marker = acknowledgementPath(
                sessionDir, metadata.channel(), metadata.windowSequence());
        try {
            return Files.isRegularFile(marker)
                    && Files.readString(marker).trim()
                        .equals(metadata.windowId());
        } catch (IOException unreadable) {
            return false;
        }
    }

    /**
     * @return true when this was an identity-aware payload and is now absent;
     *         false when it was legacy/invalid and was deliberately retained.
     */
    public static boolean deleteIfIdentityAware(Path payload) {
        if (payload == null || payload.getFileName() == null
                || payload.getParent() == null) {
            return false;
        }
        Matcher match = PAYLOAD.matcher(payload.getFileName().toString());
        if (!match.matches()) return false;

        String channel = match.group(1);
        long sequence;
        try {
            sequence = Long.parseLong(match.group(2));
        } catch (NumberFormatException malformed) {
            return false;
        }
        Path sessionDir = payload.getParent();
        Path sessionName = sessionDir.getFileName();
        if (sessionName == null) return false;
        Path metadataPath = sessionDir.resolve(
                ".gpufl-window." + channel + "." + sequence + ".json");
        if (!Files.isRegularFile(metadataPath)) {
            return false; // old client: preserve its payload
        }
        try {
            WindowMetadata metadata = JsonSettings.MAPPER.readValue(
                    metadataPath.toFile(), WindowMetadata.class);
            if (!metadata.isValidFor(
                    sessionName.toString(), channel, sequence,
                    payload.getFileName().toString())) {
                log.error("Refusing ACK cleanup: metadata {} does not match {}",
                        metadataPath, payload);
                return false;
            }
            if (!hasBackendAcknowledgement(sessionDir, metadata)) {
                return false; // old backend: no matching identity proof
            }
            Files.deleteIfExists(payload);
            log.debug("Deleted backend-ACKed payload {}; retained tombstone {}",
                    payload, metadataPath);
            return true;
        } catch (IOException | RuntimeException e) {
            log.error("Refusing ACK cleanup for {}: {}", payload,
                    e.getMessage());
            return false;
        }
    }
}
