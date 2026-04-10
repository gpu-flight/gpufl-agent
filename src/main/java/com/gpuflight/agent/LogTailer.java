package com.gpuflight.agent;

import tools.jackson.databind.JsonNode;
import com.gpuflight.agent.config.JsonSettings;
import com.gpuflight.agent.filter.DeviceMetricDeduplicator;
import com.gpuflight.agent.model.LogWrapper;
import com.gpuflight.agent.publisher.Publisher;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
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
    private final String topicPrefix;
    private final CursorManager cursorMgr;
    private final BlockingQueue<Path> consumedFilesQueue; // nullable
    private final DeviceMetricDeduplicator deduplicator;    // nullable
    private final String streamKey;

    public LogTailer(File folder, String filePrefix, String logType, String topicPrefix,
                     CursorManager cursorMgr, BlockingQueue<Path> consumedFilesQueue) {
        this(folder, filePrefix, logType, topicPrefix, cursorMgr, consumedFilesQueue, null);
    }

    public LogTailer(File folder, String filePrefix, String logType, String topicPrefix,
                     CursorManager cursorMgr, BlockingQueue<Path> consumedFilesQueue,
                     DeviceMetricDeduplicator deduplicator) {
        this.folder = folder;
        this.filePrefix = filePrefix;
        this.logType = logType;
        this.topicPrefix = topicPrefix;
        this.cursorMgr = cursorMgr;
        this.consumedFilesQueue = consumedFilesQueue;
        this.deduplicator = deduplicator;
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
        System.out.println("[" + logType + "] Active file: " + activeFile().getAbsolutePath());
        System.out.println("[" + logType + "] Active file exists: " + activeFile().exists() + ", size: " + (activeFile().exists() ? activeFile().length() : -1));

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
                    sleep(1000);
                }
                continue;
            }

            try (var reader = new RandomAccessFile(file, "r")) {
                if (offset > file.length()) offset = 0;
                reader.seek(offset);

                boolean fileIsDone = false;

                while (!Thread.currentThread().isInterrupted() && !fileIsDone) {
                    if (file.length() > offset) {
                        String line;
                        boolean publishFailed = false;
                        // NOTE: RandomAccessFile.readLine() decodes bytes as ISO-8859-1,
                        // corrupting multi-byte UTF-8 characters (e.g. em dash — becomes â€").
                        // Use readLineUtf8() instead to preserve source file content encoding.
                        while ((line = readLineUtf8(reader)) != null) {
                            if (!line.isBlank()) {
                                LogWrapper wrapper = processLine(line);
                                if (wrapper != null) {
                                    // Deduplicate device metric batches if a deduplicator is configured
                                    if (deduplicator != null && "device_metric_batch".equals(wrapper.type())) {
                                        String filtered = deduplicator.filterBatch(wrapper.data());
                                        if (filtered == null) {
                                            offset = reader.getFilePointer();
                                            continue; // all rows suppressed
                                        }
                                        if (!filtered.equals(wrapper.data())) {
                                            wrapper = new LogWrapper(wrapper.agentSendingTime(), filtered,
                                                                     wrapper.type(), wrapper.hostname(), wrapper.ipAddr());
                                        }
                                    }
                                    boolean ok = publisher.publish(topicPrefix, logType, wrapper);
                                    if (!ok) {
                                        System.out.println("[" + logType + "] Publish FAILED for " + wrapper.type() + " – will retry in 5s");
                                        // Seek back so this line is retried on next iteration
                                        reader.seek(offset);
                                        publishFailed = true;
                                        break;
                                    }
                                }
                            }
                            offset = reader.getFilePointer();
                        }
                        cursorMgr.update(streamKey, idx, offset);
                        if (publishFailed) {
                            sleep(5000); // back off before retry
                        }
                    } else {
                        // No new bytes in this file.
                        if (idx == 0) {
                            // Tailing the active file: check whether rotation just happened.
                            // On rotation the C++ client renames the active file to .1.log and
                            // creates a new empty active file. .1.log must be at least as large
                            // as our current offset (it *is* the old active, just renamed).
                            
                            // Break the inner loop to close the current file reader,
                            // then reopen it in the next outer loop iteration.
                            // This allows rotation to occur on Windows.
                            fileIsDone = true;

                            File rotated = rotatedFile(1);
                            if (rotated.exists() && rotated.length() >= offset) {
                                System.out.println("[" + logType + "] Rotation detected – switching to " + rotated.getName() + " at offset " + offset);
                                idx = 1;
                                // Intentionally keep offset so we skip bytes already sent.
                                cursorMgr.update(streamKey, idx, offset);
                            } else {
                                sleep(200);
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
                sleep(2000);
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

    /**
     * Read a line from a RandomAccessFile, decoding bytes as UTF-8.
     * <p>
     * {@link RandomAccessFile#readLine()} decodes each byte as Latin-1
     * (lower 8 bits only), which corrupts multi-byte UTF-8 characters
     * like em dash (U+2014: 0xE2 0x80 0x94 → â€").  This method reads
     * raw bytes up to a newline and decodes the buffer as UTF-8.
     * <p>
     * Handles both {@code \n} and {@code \r\n} line endings.
     * Returns null at EOF (same contract as readLine).
     */
    private static String readLineUtf8(RandomAccessFile raf) throws IOException {
        var buf = new ByteArrayOutputStream(512);
        int b;
        boolean foundAny = false;
        while ((b = raf.read()) != -1) {
            foundAny = true;
            if (b == '\n') break;
            if (b == '\r') {
                // Peek for \n after \r
                long pos = raf.getFilePointer();
                int next = raf.read();
                if (next != '\n' && next != -1) raf.seek(pos);
                break;
            }
            buf.write(b);
        }
        if (!foundAny) return null;
        return buf.toString(StandardCharsets.UTF_8);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
