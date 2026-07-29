package com.gpuflight.agent;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;

/**
 * Cross-process session liveness contract shared with gpufl-client.
 *
 * <p>The client holds an exclusive OS lock on {@value #LOCK_FILE} for the
 * complete writer lifetime. Finished root windows remain uploadable while it
 * is held; only orphan handling and session completion depend on this probe.
 */
public final class SessionOwnership {
    public static final String LOCK_FILE = ".gpufl-session.lock";
    public static final String LOSS_PREFIX = ".gpufl-transport-loss.";

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

    public static State probe(Path sessionDir) {
        Path path = sessionDir.resolve(LOCK_FILE);
        if (!Files.exists(path)) {
            return State.LEGACY_NO_LOCK;
        }
        try (FileChannel channel = FileChannel.open(
                path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            try (FileLock ignored = channel.tryLock()) {
                return ignored == null ? State.ACTIVE : State.UNOWNED;
            } catch (OverlappingFileLockException activeInThisJvm) {
                return State.ACTIVE;
            }
        } catch (IOException | RuntimeException unreadable) {
            return State.UNKNOWN;
        }
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
}
