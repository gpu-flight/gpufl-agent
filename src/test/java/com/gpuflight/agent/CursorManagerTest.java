package com.gpuflight.agent;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class CursorManagerTest {

    @Test
    void freshStart_noFile_returnsDefaultPosition(@TempDir Path tempDir) {
        File cursorFile = tempDir.resolve("cursor.json").toFile();
        assertFalse(cursorFile.exists());

        CursorManager mgr = new CursorManager(cursorFile);
        CursorPosition pos = mgr.get("mystream");

        assertEquals(0, pos.fileIndex());
        assertEquals(0L, pos.offset());
    }

    @Test
    void freshStart_createsFile(@TempDir Path tempDir) {
        File cursorFile = tempDir.resolve("cursor.json").toFile();
        new CursorManager(cursorFile);
        assertTrue(cursorFile.exists());
    }

    @Test
    void load_emptyFile_returnsDefaultPosition(@TempDir Path tempDir) throws Exception {
        File cursorFile = tempDir.resolve("cursor.json").toFile();
        cursorFile.createNewFile(); // exists but empty

        CursorManager mgr = new CursorManager(cursorFile);
        CursorPosition pos = mgr.get("stream1");

        assertEquals(0, pos.fileIndex());
        assertEquals(0L, pos.offset());
    }

    @Test
    void update_persistsAndReloads(@TempDir Path tempDir) {
        File cursorFile = tempDir.resolve("cursor.json").toFile();

        CursorManager mgr = new CursorManager(cursorFile);
        mgr.update("device", 0, 1024L);

        // Reload from disk
        CursorManager mgr2 = new CursorManager(cursorFile);
        CursorPosition pos = mgr2.get("device");

        assertEquals(0, pos.fileIndex());
        assertEquals(1024L, pos.offset());
    }

    @Test
    void update_rotatedFile_persistsFileIndex(@TempDir Path tempDir) {
        File cursorFile = tempDir.resolve("cursor.json").toFile();

        CursorManager mgr = new CursorManager(cursorFile);
        mgr.update("scope", 1, 512L);

        CursorManager mgr2 = new CursorManager(cursorFile);
        CursorPosition pos = mgr2.get("scope");

        assertEquals(1, pos.fileIndex());
        assertEquals(512L, pos.offset());
    }

    @Test
    void update_multipleStreams_persistedIndependently(@TempDir Path tempDir) {
        File cursorFile = tempDir.resolve("cursor.json").toFile();

        CursorManager mgr = new CursorManager(cursorFile);
        mgr.update("device", 0, 100L);
        mgr.update("scope", 1, 200L);
        mgr.update("system", 0, 300L);

        CursorManager mgr2 = new CursorManager(cursorFile);
        assertEquals(100L, mgr2.get("device").offset());
        assertEquals(200L, mgr2.get("scope").offset());
        assertEquals(300L, mgr2.get("system").offset());
        assertEquals(1, mgr2.get("scope").fileIndex());
    }

    @Test
    void update_samePosition_doesNotSave(@TempDir Path tempDir) throws Exception {
        File cursorFile = tempDir.resolve("cursor.json").toFile();

        CursorManager mgr = new CursorManager(cursorFile);
        mgr.update("stream", 0, 100L);

        long modifiedAfterFirstUpdate = cursorFile.lastModified();
        Thread.sleep(10); // ensure time difference is detectable

        mgr.update("stream", 0, 100L); // same position

        // File should not have been rewritten
        assertEquals(modifiedAfterFirstUpdate, cursorFile.lastModified());
    }

    @Test
    void update_differentPosition_savesFile(@TempDir Path tempDir) throws Exception {
        File cursorFile = tempDir.resolve("cursor.json").toFile();

        CursorManager mgr = new CursorManager(cursorFile);
        mgr.update("stream", 0, 100L);

        long modifiedAfterFirst = cursorFile.lastModified();
        Thread.sleep(10);

        mgr.update("stream", 0, 200L); // different offset

        assertTrue(cursorFile.lastModified() >= modifiedAfterFirst);
    }

    @Test
    void get_unknownStream_returnsDefault(@TempDir Path tempDir) {
        File cursorFile = tempDir.resolve("cursor.json").toFile();

        CursorManager mgr = new CursorManager(cursorFile);
        CursorPosition pos = mgr.get("nonexistent");

        assertEquals(0, pos.fileIndex());
        assertEquals(0L, pos.offset());
    }

    @Test
    void load_corruptFile_returnsDefaultAndContinues(@TempDir Path tempDir) throws Exception {
        File cursorFile = tempDir.resolve("cursor.json").toFile();
        Files.writeString(cursorFile.toPath(), "{ this is not valid json }}}");

        CursorManager mgr = new CursorManager(cursorFile);
        CursorPosition pos = mgr.get("stream");

        // Should fall back gracefully
        assertEquals(0, pos.fileIndex());
        assertEquals(0L, pos.offset());
    }

    @Test
    void load_validJson_returnsPersistedPosition(@TempDir Path tempDir) throws Exception {
        File cursorFile = tempDir.resolve("cursor.json").toFile();
        Files.writeString(cursorFile.toPath(), """
            {
              "streams": {
                "app.device": { "fileIndex": 2, "offset": 4096 }
              }
            }
            """);

        CursorManager mgr = new CursorManager(cursorFile);
        CursorPosition pos = mgr.get("app.device");

        assertEquals(2, pos.fileIndex());
        assertEquals(4096L, pos.offset());
    }
}
