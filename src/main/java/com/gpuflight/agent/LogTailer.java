package com.gpuflight.agent;

import tools.jackson.databind.JsonNode;
import com.gpuflight.agent.config.JsonSettings;
import com.gpuflight.agent.model.LogWrapper;
import com.gpuflight.agent.publisher.Publisher;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;

/**
 * Tails log files written by the C++ gpufl client.
 *
 * File naming convention produced by the C++ LogFileRotator:
 *   Active file  : <prefix>.<channel>.log         (no numeric suffix; always being written)
 *   Rotated files: <prefix>.<channel>.1.log        (most recently rotated)
 *                  <prefix>.<channel>.2.log        (older), …
 *
 * Cursor fileIndex semantics:
 *   0   → currently tailing the active file
 *   N≥1 → finishing the just-rotated file .<N>.log after a rotation event,
 *          then returning to index 0 for the new active file
 */
public class LogTailer {
    private final File folder;
    private final String filePrefix;
    private final String logType;
    private final CursorManager cursorMgr;
    private final BlockingQueue<Path> consumedFilesQueue; // nullable
    private final String streamKey;

    public LogTailer(File folder, String filePrefix, String logType,
                     CursorManager cursorMgr, BlockingQueue<Path> consumedFilesQueue) {
        this.folder = folder;
        this.filePrefix = filePrefix;
        this.logType = logType;
        this.cursorMgr = cursorMgr;
        this.consumedFilesQueue = consumedFilesQueue;
        this.streamKey = filePrefix + "." + logType;
    }

    /** The active file written by the C++ client: <prefix>.<type>.log */
    private File activeFile() {
        return new File(folder, filePrefix + "." + logType + ".log");
    }

    /** A completed, rotated file: <prefix>.<type>.<index>.log  (index ≥ 1) */
    private File rotatedFile(int index) {
        return new File(folder, filePrefix + "." + logType + "." + index + ".log");
    }

    /**
     * Blocking tail loop — designed to run on a virtual thread.
     * Thread.sleep() parks the virtual thread cheaply; no carrier thread is blocked.
     */
    public void tail(Publisher publisher) {
        var cursor = cursorMgr.get(streamKey);
        int idx = cursor.fileIndex();
        long offset = cursor.offset();

        System.out.println("[" + logType + "] Starting – fileIndex=" + idx + ", offset=" + offset);

        while (!Thread.currentThread().isInterrupted()) {
            File file = idx == 0 ? activeFile() : rotatedFile(idx);

            if (!file.exists()) {
                if (idx > 0) {
                    // The rotated file has already been removed (e.g. agent restarted long
                    // after the rotation completed and the file was aged out).
                    // Fall back to the active file to avoid getting stuck.
                    System.out.println("[" + logType + "] Rotated file " + file.getName() + " gone, resuming active file.");
                    idx = 0;
                    offset = 0;
                    cursorMgr.update(streamKey, idx, offset);
                } else {
                    System.out.println("[" + logType + "] Waiting for active file: " + file.getAbsolutePath() + "…");
                    sleep(2000);
                }
                continue;
            }

            System.out.println("[" + logType + "] Reading: " + file.getName());

            try (var reader = new RandomAccessFile(file, "r")) {
                if (offset > file.length()) offset = 0;
                reader.seek(offset);

                boolean fileIsDone = false;

                while (!Thread.currentThread().isInterrupted() && !fileIsDone) {
                    long fileLen = file.length();

                    if (fileLen > offset) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            if (!line.isBlank()) {
                                LogWrapper wrapper = processLine(line);
                                if (wrapper != null) {
                                    publisher.publish("gpu-trace-events", logType, wrapper);
                                }
                            }
                            offset = reader.getFilePointer();
                        }
                        cursorMgr.update(streamKey, idx, offset);
                    } else {
                        // No new bytes in this file.
                        if (idx == 0) {
                            // Tailing the active file: check whether rotation just happened.
                            // On rotation the C++ client renames the active file to .1.log and
                            // creates a new empty active file. .1.log must be at least as large
                            // as our current offset (it *is* the old active, just renamed).
                            File rotated = rotatedFile(1);
                            if (rotated.exists() && rotated.length() >= offset) {
                                System.out.println("[" + logType + "] Rotation detected – switching to " + rotated.getName() + " at offset " + offset);
                                idx = 1;
                                // Intentionally keep offset so we skip bytes already sent.
                                cursorMgr.update(streamKey, idx, offset);
                                fileIsDone = true;
                            } else {
                                sleep(100);
                            }
                        } else {
                            // Finished draining the rotated file.
                            // Offer path for archival before returning to the active file.
                            System.out.println("[" + logType + "] Finished " + file.getName() + ", resuming active file.");
                            if (consumedFilesQueue != null) {
                                consumedFilesQueue.offer(file.toPath());
                            }
                            idx = 0;
                            offset = 0;
                            cursorMgr.update(streamKey, idx, offset);
                            fileIsDone = true;
                        }
                    }
                }
            } catch (IOException e) {
                System.out.println("[" + logType + "] IO error: " + e.getMessage());
                sleep(1000);
            }
        }
    }

    private LogWrapper processLine(String rawLine) {
        try {
            JsonNode node = JsonSettings.MAPPER.readTree(rawLine);
            String type = node.path("type").asText("unknown");
            InetAddress inet = InetAddress.getLocalHost();
            return new LogWrapper(
                System.currentTimeMillis(),
                rawLine,
                type,
                inet.getHostName(),
                inet.getHostAddress()
            );
        } catch (Exception e) {
            System.out.println("Failed to parse line: " + e.getMessage());
            return null;
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
