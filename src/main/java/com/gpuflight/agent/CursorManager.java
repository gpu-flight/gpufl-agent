package com.gpuflight.agent;

import com.gpuflight.agent.config.JsonSettings;

import java.io.File;

public class CursorManager {
    private final File cursorFile;
    private final CursorState state;

    public CursorManager(File cursorFile) {
        this.cursorFile = cursorFile;
        this.state = load();
    }

    private CursorState load() {
        if (!cursorFile.exists()) {
            try { if(!cursorFile.createNewFile()) return new CursorState(); } catch (Exception ignored) {}
            return new CursorState();
        }
        try {
            String text = java.nio.file.Files.readString(cursorFile.toPath()).strip();
            if (text.isEmpty()) return new CursorState();
            return JsonSettings.MAPPER.readValue(text, CursorState.class);
        } catch (Exception e) {
            System.out.println("Corrupt cursor file. Starting fresh.");
            try { if(!cursorFile.createNewFile()) return new CursorState(); } catch (Exception ignored) {}
            return new CursorState();
        }
    }

    public synchronized void save() {
        try {
            var tempFile = new File(cursorFile.getAbsolutePath() + ".tmp");
            JsonSettings.MAPPER.writerWithDefaultPrettyPrinter().writeValue(tempFile, state);
            if (cursorFile.exists()) cursorFile.delete();
            tempFile.renameTo(cursorFile);
        } catch (Exception e) {
            System.out.println("Failed to save cursor: " + e.getMessage());
        }
    }

    /**
     * Update offset without changing recorded file identity. Preserves any
     * identity already stored for this stream so identity-less call sites
     * don't wipe it.
     */
    public void update(String streamKey, int fileIndex, long offset) {
        var oldPos = state.streams().get(streamKey);
        String fileKey = oldPos != null ? oldPos.fileKey() : null;
        long fileSize = oldPos != null ? oldPos.fileSize() : 0L;
        long headSig = oldPos != null ? oldPos.headSig() : 0L;
        update(streamKey, fileIndex, offset, fileKey, fileSize, headSig);
    }

    /** Update offset and record the identity of the file being read. */
    public void update(String streamKey, int fileIndex, long offset,
                       String fileKey, long fileSize, long headSig) {
        var newPos = new CursorPosition(fileIndex, offset, fileKey, fileSize, headSig);
        var oldPos = state.streams().get(streamKey);
        if (!newPos.equals(oldPos)) {
            state.streams().put(streamKey, newPos);
            save();
        }
    }

    public CursorPosition get(String streamKey) {
        return state.streams().getOrDefault(streamKey, new CursorPosition(0, 0L));
    }
}
