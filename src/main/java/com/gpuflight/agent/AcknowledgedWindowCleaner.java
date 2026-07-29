package com.gpuflight.agent;

import com.gpuflight.agent.config.JsonSettings;
import com.gpuflight.agent.model.WindowMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
