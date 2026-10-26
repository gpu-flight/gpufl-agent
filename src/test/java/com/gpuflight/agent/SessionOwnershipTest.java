package com.gpuflight.agent;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

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
    void writerLockInAnotherProcessProbesActive(@TempDir Path session)
            throws Exception {
        Path lockPath = session.resolve(SessionOwnership.LOCK_FILE);
        Files.writeString(lockPath, "");
        Process holder = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"),
                LockHolder.class.getName(), lockPath.toString())
                .redirectError(ProcessBuilder.Redirect.INHERIT)
                .start();
        try (BufferedReader out = new BufferedReader(new InputStreamReader(
                holder.getInputStream(), StandardCharsets.UTF_8))) {
            assertEquals("locked", out.readLine());
            assertEquals(SessionOwnership.State.ACTIVE,
                    SessionOwnership.probe(session));
        } finally {
            holder.getOutputStream().close();
            assertTrue(holder.waitFor(30, TimeUnit.SECONDS));
        }
        assertEquals(SessionOwnership.State.UNOWNED,
                SessionOwnership.probe(session));
    }

    @Test
    @EnabledOnOs({OS.LINUX, OS.MAC})
    void readOnlyLockFileStillProbesUnowned(@TempDir Path session)
            throws Exception {
        Path lockPath = session.resolve(SessionOwnership.LOCK_FILE);
        Files.writeString(lockPath, "");
        // What an agent in the client's group sees under umask 022.
        Files.setPosixFilePermissions(
                lockPath, PosixFilePermissions.fromString("r--------"));
        assumeFalse(Files.isWritable(lockPath), "file modes do not bind root");
        assertEquals(SessionOwnership.State.UNOWNED,
                SessionOwnership.probe(session));

        Files.setPosixFilePermissions(
                lockPath, PosixFilePermissions.fromString("---------"));
        assertEquals(SessionOwnership.State.UNKNOWN,
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

    /**
     * A writer in another process: an exclusive lock on byte 0, the range the
     * Windows client locks, held until stdin closes.
     */
    static final class LockHolder {
        public static void main(String[] args) throws Exception {
            try (FileChannel channel = FileChannel.open(Path.of(args[0]),
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
                 FileLock ignored = channel.lock(0L, 1L, false)) {
                System.out.println("locked");
                System.out.flush();
                System.in.read();
            }
        }
    }
}
