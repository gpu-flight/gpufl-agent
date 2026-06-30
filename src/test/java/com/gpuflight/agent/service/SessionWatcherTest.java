package com.gpuflight.agent.service;

import com.gpuflight.agent.model.DiscoveredSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SessionWatcher discovers sessions both flat under the watched folder
 * ({@code <folder>/<session_id>/} - embedded gpufl and single-pass `gpufl trace`)
 * and nested one grouping level deep
 * ({@code <folder>/run-<app>-<analysis_id>/<session_id>/} - a multi-pass run).
 * Both must be found so "watch a parent, run many times" keeps working across
 * single- and multi-pass runs.
 */
class SessionWatcherTest {

    private static final List<String> TYPES = List.of("device", "scope", "system", "sass");

    private static void session(Path dir, String channelFile) throws IOException {
        Files.createDirectories(dir);
        Files.writeString(dir.resolve(channelFile), "{}\n");
    }

    @Test
    void discoversFlatSessionDirectlyUnderFolder(@TempDir Path root) throws IOException {
        session(root.resolve("sess-aaa"), "device.log");

        List<DiscoveredSession> found = SessionWatcher.discoverSources(root.toFile(), TYPES);

        assertEquals(1, found.size());
        assertEquals("sess-aaa", found.get(0).sessionId());
        assertEquals(root.toFile(), found.get(0).folder());
    }

    @Test
    void discoversNestedSessionsUnderRunFolder(@TempDir Path root) throws IOException {
        Path group = root.resolve("run-myapp-abc12345");
        session(group.resolve("sess-pass0"), "device.log");
        session(group.resolve("sess-pass1"), "sass.1.log.gz");

        List<DiscoveredSession> found = SessionWatcher.discoverSources(root.toFile(), TYPES);

        assertEquals(2, found.size());
        assertEquals(List.of("sess-pass0", "sess-pass1"),
                found.stream().map(DiscoveredSession::sessionId).sorted().toList());
        // <folder>/<session_id> must resolve to the real pass directory.
        for (DiscoveredSession s : found) {
            assertTrue(new File(s.folder(), s.sessionId()).isDirectory());
        }
    }

    @Test
    void discoversFlatAndNestedTogether(@TempDir Path root) throws IOException {
        session(root.resolve("flat-sess"), "system.log");
        session(root.resolve("run-app-deadbeef").resolve("grouped-sess"), "device.2.log");

        List<DiscoveredSession> found = SessionWatcher.discoverSources(root.toFile(), TYPES);

        assertEquals(2, found.size());
        assertEquals(List.of("flat-sess", "grouped-sess"),
                found.stream().map(DiscoveredSession::sessionId).sorted().toList());
    }

    @Test
    void ignoresDirsWithoutChannelFilesAndDotDirs(@TempDir Path root) throws IOException {
        Files.createDirectories(root.resolve("not-a-session").resolve("random"));
        Files.writeString(root.resolve("not-a-session").resolve("notes.txt"), "x");
        session(root.resolve(".hidden"), "device.log");          // dot dir is skipped
        session(root.resolve("real"), "scope.log");

        List<DiscoveredSession> found = SessionWatcher.discoverSources(root.toFile(), TYPES);

        assertEquals(1, found.size());
        assertEquals("real", found.get(0).sessionId());
    }

    @Test
    void doesNotRecurseBeyondOneGroupingLevel(@TempDir Path root) throws IOException {
        // <root>/a/b/<session>/ is two grouping levels deep - out of scope.
        session(root.resolve("a").resolve("b").resolve("too-deep"), "device.log");

        List<DiscoveredSession> found = SessionWatcher.discoverSources(root.toFile(), TYPES);

        assertTrue(found.isEmpty());
    }

    @Test
    void skipsSessionsMarkedUploadedOrFailed(@TempDir Path root) throws IOException {
        session(root.resolve("done"), "device.log");
        Files.writeString(root.resolve("done").resolve(SessionWatcher.UPLOADED_MARKER), "");
        session(root.resolve("failed"), "device.log");
        Files.writeString(root.resolve("failed").resolve(SessionWatcher.FAILED_MARKER), "");
        session(root.resolve("pending"), "device.log");

        List<DiscoveredSession> found = SessionWatcher.discoverSources(root.toFile(), TYPES);

        assertEquals(1, found.size());
        assertEquals("pending", found.get(0).sessionId());
    }
}
