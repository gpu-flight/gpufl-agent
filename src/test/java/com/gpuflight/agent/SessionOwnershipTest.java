package com.gpuflight.agent;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

class SessionOwnershipTest {

    @Test
    void osLockOverridesQuietDirectoryHeuristics(@TempDir Path session) throws Exception {
        assertEquals(SessionOwnership.State.LEGACY_NO_LOCK,
                SessionOwnership.probe(session));

        Path lockPath = session.resolve(SessionOwnership.LOCK_FILE);
        Files.writeString(lockPath, "");
        assertEquals(SessionOwnership.State.UNOWNED,
                SessionOwnership.probe(session));

        try (FileChannel channel = FileChannel.open(
                lockPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
             FileLock ignored = channel.lock()) {
            assertEquals(SessionOwnership.State.ACTIVE,
                    SessionOwnership.probe(session));
        }

        assertEquals(SessionOwnership.State.UNOWNED,
                SessionOwnership.probe(session));
    }

    @Test
    void transportLossMarkerIsDurableCompletionGate(@TempDir Path session)
            throws Exception {
        assertFalse(SessionOwnership.hasTransportLoss(session));
        Files.writeString(
                session.resolve(".gpufl-transport-loss.device.9.json"), "{}");
        assertTrue(SessionOwnership.hasTransportLoss(session));
    }
}
