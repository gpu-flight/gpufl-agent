package com.gpuflight.agent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Cross-process session liveness contract shared with gpufl-client.
 *
 * <p>The client holds an exclusive OS lock on {@value #LOCK_FILE} for the
 * complete writer lifetime. Finished root windows remain uploadable while it
 * is held; only orphan handling and session completion depend on this probe.
 */
public final class SessionOwnership {
    private static final Logger log =
            LoggerFactory.getLogger(SessionOwnership.class);
    public static final String LOCK_FILE = ".gpufl-session.lock";
    public static final String LOSS_PREFIX = ".gpufl-transport-loss.";
    private static final Set<Path> UNREADABLE_WARNED =
            ConcurrentHashMap.newKeySet();

    public enum State {
        /** Another live process owns the session. */
        ACTIVE,
        /** The lock file exists and its OS lock is currently acquirable. */
        UNOWNED,
        /** Produced by a client older than the ownership-lock contract. */
        LEGACY_NO_LOCK,
        /** Conservatively treated as active; never finalize through an I/O error. */
        UNKNOWN
    }

    private SessionOwnership() {}

    /**
     * Probes with a shared lock on a read-only channel, so an agent running as
     * another user needs only read access to the lock file. A shared lock
     * still conflicts with the writer's exclusive one (fcntl on POSIX,
     * LockFileEx on Windows).
     */
    public static State probe(Path sessionDir) {
        Path path = sessionDir.resolve(LOCK_FILE);
        if (!Files.exists(path)) {
            return State.LEGACY_NO_LOCK;
        }
        FileChannel opened;
        try {
            opened = FileChannel.open(path, StandardOpenOption.READ);
        } catch (IOException | RuntimeException unreadable) {
            warnUnreadableOnce(sessionDir, path, unreadable);
            return State.UNKNOWN;
        }
        try (FileChannel channel = opened) {
            try (FileLock ignored = channel.tryLock(0L, Long.MAX_VALUE, true)) {
                return ignored == null ? State.ACTIVE : State.UNOWNED;
            } catch (OverlappingFileLockException activeInThisJvm) {
                return State.ACTIVE;
            }
        } catch (IOException | RuntimeException lockFailure) {
            return State.UNKNOWN;
        }
    }

    private static void warnUnreadableOnce(
            Path sessionDir, Path lockPath, Exception cause) {
        if (!UNREADABLE_WARNED.add(sessionDir.toAbsolutePath().normalize())) {
            return;
        }
        log.warn("Cannot open session lock {} ({}); treating the session as "
                        + "active. If the agent runs as another user, its "
                        + "group needs read access to the lock file and write "
                        + "access to the session directory (the agent writes "
                        + "markers and tombstones and deletes payloads there). "
                        + "Clients up to 1.3.0rc6 create the lock owner-only "
                        + "and need chmod g+rw on it.",
                lockPath, cause.toString());
    }

    public static boolean hasTransportLoss(Path sessionDir) {
        try (Stream<Path> entries = Files.list(sessionDir)) {
            return entries.anyMatch(path ->
                path.getFileName().toString().startsWith(LOSS_PREFIX));
        } catch (IOException e) {
            // Completion through an unreadable directory is unsafe.
            return true;
        }
    }

    public static Path agentWindowFailureMarker(
            Path sessionDir, String channel, int sequence) {
        String safeChannel = channel == null
                ? "unknown"
                : channel.replaceAll("[^A-Za-z0-9_-]", "_");
        return sessionDir.resolve(
                LOSS_PREFIX + "agent-" + safeChannel + "." + sequence + ".json");
    }

    public static boolean hasAgentWindowFailure(
            Path sessionDir, String channel, int sequence) {
        return Files.isRegularFile(
                agentWindowFailureMarker(sessionDir, channel, sequence));
    }

    /**
     * Persist a terminal local identity failure. The payload and sidecar stay
     * in place for forensic inspection; the shared loss prefix prevents the
     * session from being reported complete.
     */
    public static boolean recordAgentWindowFailure(
            Path sessionDir, String channel, int sequence, String reason) {
        Path marker = agentWindowFailureMarker(sessionDir, channel, sequence);
        if (Files.isRegularFile(marker)) return true;
        String json = "{\"schema_version\":1,"
                + "\"source\":\"gpufl-agent\","
                + "\"channel\":\"" + jsonEscape(channel) + "\","
                + "\"window_sequence\":" + sequence + ","
                + "\"reason\":\"" + jsonEscape(reason) + "\"}\n";
        try {
            Files.createDirectories(sessionDir);
            try (FileChannel out = FileChannel.open(
                    marker, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE)) {
                ByteBuffer bytes = StandardCharsets.UTF_8.encode(json);
                while (bytes.hasRemaining()) out.write(bytes);
                out.force(true);
            }
            return true;
        } catch (FileAlreadyExistsException alreadyRecorded) {
            return Files.isRegularFile(marker);
        } catch (IOException e) {
            return false;
        }
    }

    private static String jsonEscape(String value) {
        if (value == null) return "";
        return value.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\r", "\\r")
                .replace("\n", "\\n");
    }
}
