package com.gpuflight.agent;

import tools.jackson.databind.JsonNode;
import com.gpuflight.agent.config.JsonSettings;
import com.gpuflight.agent.filter.DeviceMetricDeduplicator;
import com.gpuflight.agent.model.LogWrapper;
import com.gpuflight.agent.publisher.Publisher;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.BlockingQueue;
import java.util.zip.CRC32;
import java.util.zip.GZIPInputStream;

/**
 * Tails log files written by the C++ gpufl client.
 *
 * v1.2 file naming (one LogTailer per session/channel pair):
 *   Active file  : <folder>/<sessionId>/<channel>.log
 *   Rotated files: <folder>/<sessionId>/<channel>.1.log[.gz]   (newest rotated)
 *                  <folder>/<sessionId>/<channel>.2.log[.gz]   (older), …
 *
 * Cursor fileIndex semantics:
 *   0   → currently tailing the active file
 *   N≥1 → finishing the just-rotated file .<N>.log[.gz] after a rotation event,
 *          then returning to index 0 for the new active file
 *
 * Two robustness features guard against the client compressing a rotated file
 * before the agent finished reading it:
 *   A) the agent can read a rotated file whether it is .log OR .log.gz, and
 *   D) on restart it locates the file it was reading by a content signature
 *      (which survives both rename and compression) instead of blindly
 *      resetting to the new active file.
 *
 * Migration from v1.1: pre-v1.2 used a flat layout
 * <folder>/<prefix>.<channel>.log[.N.log[.gz]] (one tailer per
 * <prefix>.<channel> pair). v1.2 nests each session in its own
 * subdirectory; one tailer per <sessionId>.<channel> pair instead. The
 * tailer's API is unchanged, just the path construction.
 */
public class LogTailer {
    private final File folder;
    private final String sessionId;
    private final String logType;
    private final String topicPrefix;
    private final CursorManager cursorMgr;
    private final BlockingQueue<Path> consumedFilesQueue; // nullable
    private final DeviceMetricDeduplicator deduplicator;    // nullable
    private final String streamKey;

    /** Max rotated index scanned during restart reconciliation. Matches the
     *  client's LogRotationOptions.max_files default. */
    private static final int MAX_ROTATED_SCAN = 100;
    /** Bytes of uncompressed head used for the content signature. */
    private static final int HEAD_SIG_BYTES = 512;

    public LogTailer(File folder, String sessionId, String logType, String topicPrefix,
                     CursorManager cursorMgr, BlockingQueue<Path> consumedFilesQueue) {
        this(folder, sessionId, logType, topicPrefix, cursorMgr, consumedFilesQueue, null);
    }

    public LogTailer(File folder, String sessionId, String logType, String topicPrefix,
                     CursorManager cursorMgr, BlockingQueue<Path> consumedFilesQueue,
                     DeviceMetricDeduplicator deduplicator) {
        this.folder = folder;
        this.sessionId = sessionId;
        this.logType = logType;
        this.topicPrefix = topicPrefix;
        this.cursorMgr = cursorMgr;
        this.consumedFilesQueue = consumedFilesQueue;
        this.deduplicator = deduplicator;
        // Cursor key namespaces by session_id so two concurrent sessions'
        // device channels (say `s1.device` and `s2.device`) don't share
        // a cursor position. Survives restart — sessions in
        // cursor.json that are no longer on disk get pruned lazily.
        this.streamKey = sessionId + "." + logType;
    }

    /** The session subdirectory under the watched folder. */
    private File sessionDir() {
        return new File(folder, sessionId);
    }

    /** v1.2 active file: <folder>/<sessionId>/<channel>.log */
    private File activeFile() {
        return new File(sessionDir(), logType + ".log");
    }

    /**
     * The compressed-active file: {@code <folder>/<sessionId>/<channel>.log.gz}.
     * On a CLEAN SHUTDOWN the client compresses the active file IN PLACE — the
     * name keeps the channel base with NO rotation index (active {@code
     * <channel>.log} → {@code <channel>.log.gz}), distinct from a rotated
     * {@code <channel>.<N>.log.gz}. A finished session's only data for a channel
     * therefore lives here, and there is never a plain {@code <channel>.log}
     * alongside it. The tail loop falls back to this when the active file is
     * gone so finished sessions are still ingested.
     */
    private File activeGzFile() {
        return new File(sessionDir(), logType + ".log.gz");
    }

    /**
     * The rotated file for {@code index} (≥1). Prefers the uncompressed
     * {@code .index.log}; falls back to {@code .index.log.gz} once the client
     * has compressed it. Returns null if neither exists.
     */
    private File resolveRotated(int index) {
        File plain = new File(sessionDir(), logType + "." + index + ".log");
        if (plain.exists()) return plain;
        File gz = new File(sessionDir(), logType + "." + index + ".log.gz");
        return gz.exists() ? gz : null;
    }

    private static boolean isGz(File f) {
        return f.getName().endsWith(".gz");
    }

    // ---- File identity (D) ----

    private record Identity(String fileKey, long size, long headSig) {}

    private static Identity identityOf(File f) {
        String fileKey = null;
        long size = 0L;
        try {
            BasicFileAttributes a = Files.readAttributes(f.toPath(), BasicFileAttributes.class);
            fileKey = a.fileKey() == null ? null : a.fileKey().toString();
            size = a.size();
        } catch (IOException ignored) {
            // best-effort; signature below still identifies the file
        }
        return new Identity(fileKey, size, headSignature(f));
    }

    /**
     * CRC32 of the first {@link #HEAD_SIG_BYTES} bytes of the file's
     * UNCOMPRESSED content (transparently decompresses a .gz). Because it is
     * computed over content, it survives both the client's rename AND its
     * compression — it is the signal that lets a restart find the file it was
     * reading even after it became a .gz. Returns 0 when unreadable (the
     * "unset" sentinel); never returns 0 for real content.
     */
    private static long headSignature(File f) {
        try (InputStream raw = new FileInputStream(f);
             InputStream in = isGz(f) ? new GZIPInputStream(new BufferedInputStream(raw))
                                      : new BufferedInputStream(raw)) {
            byte[] buf = new byte[HEAD_SIG_BYTES];
            int n = 0, r;
            while (n < buf.length && (r = in.read(buf, n, buf.length - n)) != -1) n += r;
            if (n <= 0) return 0L;
            CRC32 crc = new CRC32();
            crc.update(buf, 0, n);
            long v = crc.getValue();
            return v == 0 ? 1L : v;
        } catch (IOException e) {
            return 0L;
        }
    }

    /** Whether {@code cand} is the file described by the saved cursor identity. */
    private static boolean identityMatches(CursorPosition saved, Identity cand) {
        if (saved.headSig() != 0L && cand.headSig() == saved.headSig()) return true;       // compression-proof
        if (saved.fileKey() != null && saved.fileKey().equals(cand.fileKey())) return true; // uncompressed fast path
        return false;
    }

    /** Locate the index (0=active, ≥1=rotated) whose current file matches the
     *  saved identity. Returns -1 if no file matches. */
    private int locateByIdentity(CursorPosition saved) {
        File active = activeFile();
        if (active.exists() && identityMatches(saved, identityOf(active))) return 0;
        // The active file we were reading may have been compressed in place to
        // <channel>.log.gz at shutdown. Its content signature is unchanged
        // (headSignature decompresses transparently), so it still resolves to
        // the active slot (index 0) and the loop's active branch drains it.
        File activeGz = activeGzFile();
        if (activeGz.exists() && identityMatches(saved, identityOf(activeGz))) return 0;
        for (int i = 1; i <= MAX_ROTATED_SCAN; i++) {
            File r = resolveRotated(i);
            if (r == null) continue;
            if (identityMatches(saved, identityOf(r))) return i;
        }
        return -1;
    }

    /**
     * Blocking tail loop — designed to run on a virtual thread.
     * Thread.sleep() parks the virtual thread cheaply; no carrier thread is blocked.
     */
    public void tail(Publisher publisher) {
        var cursor = cursorMgr.get(streamKey);
        int idx = cursor.fileIndex();
        long offset = cursor.offset();

        // ---- D: restart reconciliation ----
        // A brief downtime may have rotated (and compressed) the file we were
        // reading out of its slot — active → .1.log → .1.log.gz, or a rotated
        // file shifted to a higher index. If we recorded an identity, find the
        // file that currently holds it (by content signature, which survives
        // compression) and resume from there instead of resetting to the new
        // active file and losing the unread tail.
        if (cursor.headSig() != 0L || cursor.fileKey() != null) {
            int located = locateByIdentity(cursor);
            if (located >= 0 && located != idx) {
                System.out.println("[" + logType + "] Restart: saved file is now index " + located
                        + " (cursor said " + idx + "); resuming there at offset " + offset);
                idx = located;
                cursorMgr.update(streamKey, idx, offset);
            }
            // located == -1 → file not found anywhere; keep cursor, legacy fallback applies.
        }

        System.out.println("[" + logType + "] Starting – fileIndex=" + idx + ", offset=" + offset);
        System.out.println("[" + logType + "] Active file: " + activeFile().getAbsolutePath());
        System.out.println("[" + logType + "] Active file exists: " + activeFile().exists() + ", size: " + (activeFile().exists() ? activeFile().length() : -1));

        // Signature of the most recently fully-consumed rotated file. Prevents
        // re-switching to (and re-reading) the same rotated file after we drain
        // it and return to the active file while it is still on disk (it lingers
        // until the client's next rotation shifts it, or the archiver deletes it).
        long lastConsumedSig = 0L;

        while (!Thread.currentThread().isInterrupted()) {
            File file = (idx == 0) ? activeFile() : resolveRotated(idx);

            if (idx > 0 && file == null) {
                // The rotated file has aged out (e.g. agent restarted long after
                // the rotation and the file was deleted). Fall back to active.
                System.out.println("[" + logType + "] Rotated index " + idx + " gone, resuming active file.");
                idx = 0;
                offset = 0;
                cursorMgr.update(streamKey, idx, offset);
                continue;
            }
            if (idx == 0 && !file.exists()) {
                // The active <channel>.log is gone. On a clean shutdown the
                // client compresses the active file IN PLACE to
                // <channel>.log.gz (no rotation index), so a finished session's
                // only data for this channel lives there. Drain it once from
                // our current offset: drainGz skips already-read uncompressed
                // bytes, so this is correct whether we discovered the session
                // cold (offset 0 → whole file) or were live-tailing
                // <channel>.log when it was compressed at shutdown (offset =
                // bytes already published → just the tail).
                File activeGz = activeGzFile();
                if (activeGz.exists()) {
                    long resume = drainGz(activeGz, offset, publisher, 0);
                    if (resume < 0) {
                        System.out.println("[" + logType + "] Drained compressed-active "
                                + activeGz.getName() + " – session finished.");
                        if (consumedFilesQueue != null) consumedFilesQueue.offer(activeGz.toPath());
                        return; // compressed-active is terminal: no more data will arrive
                    }
                    // Publish failed mid-drain — back off, retry from persisted offset.
                    offset = resume;
                    sleep(5000);
                    continue;
                }
                sleep(1000);
                continue;
            }

            // ---- A: drain a compressed rotated file ----
            if (idx > 0 && isGz(file)) {
                long resume = drainGz(file, offset, publisher, idx);
                if (resume < 0) {
                    System.out.println("[" + logType + "] Finished " + file.getName() + " (gz), resuming active file.");
                    if (consumedFilesQueue != null) consumedFilesQueue.offer(file.toPath());
                    lastConsumedSig = headSignature(file);
                    idx = 0;
                    offset = 0;
                    cursorMgr.update(streamKey, idx, offset);
                } else {
                    // Publish failed mid-gz — back off and retry from persisted offset.
                    offset = resume;
                    sleep(5000);
                }
                continue;
            }

            // ---- plain .log path (active file, or an uncompressed rotated file) ----
            Identity id = identityOf(file);
            try (var reader = new RandomAccessFile(file, "r")) {
                if (offset > file.length()) offset = 0;
                reader.seek(offset);

                boolean fileIsDone = false;

                while (!Thread.currentThread().isInterrupted() && !fileIsDone) {
                    if (file.length() > offset) {
                        String line;
                        boolean publishFailed = false;
                        // NOTE: RandomAccessFile.readLine() decodes bytes as ISO-8859-1,
                        // corrupting multi-byte UTF-8 characters. Use readLineUtf8().
                        while ((line = readLineUtf8(reader)) != null) {
                            long lineStart = offset;
                            if (!handleLine(line, publisher)) {
                                System.out.println("[" + logType + "] Publish FAILED – will retry in 5s");
                                reader.seek(lineStart); // retry this line next iteration
                                publishFailed = true;
                                break;
                            }
                            offset = reader.getFilePointer();
                        }
                        cursorMgr.update(streamKey, idx, offset, id.fileKey(), id.size(), id.headSig());
                        if (publishFailed) {
                            sleep(5000);
                        }
                    } else {
                        // No new bytes in this file.
                        if (idx == 0) {
                            // Tailing the active file: check whether rotation just happened.
                            // Break to close the reader so rotation can proceed on Windows.
                            fileIsDone = true;

                            File rotated = resolveRotated(1);
                            // Don't re-consume a rotated file we already fully drained: it
                            // lingers on disk until the client's next rotation or the archiver
                            // removes it. Identify it by content signature.
                            long rsig = (rotated != null) ? headSignature(rotated) : 0L;
                            if (rotated != null && rsig != 0L && rsig == lastConsumedSig) {
                                sleep(200); // already consumed this exact rotated content; wait for a real new rotation
                            }
                            // A compressed rotated file (.gz) is always valid to switch to —
                            // its uncompressed size isn't comparable to our byte offset, so we
                            // rely on skip-to-offset in drainGz. For an uncompressed .log we keep
                            // the length>=offset guard (it must be at least the old active size).
                            else if (rotated != null && (isGz(rotated) || rotated.length() >= offset)) {
                                System.out.println("[" + logType + "] Rotation detected – switching to " + rotated.getName() + " at offset " + offset);
                                idx = 1;
                                cursorMgr.update(streamKey, idx, offset, id.fileKey(), id.size(), id.headSig());
                            } else {
                                sleep(200);
                            }
                        } else {
                            // Finished draining an uncompressed rotated file.
                            System.out.println("[" + logType + "] Finished " + file.getName() + ", resuming active file.");
                            if (consumedFilesQueue != null) {
                                consumedFilesQueue.offer(file.toPath());
                            }
                            lastConsumedSig = id.headSig();
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

    /**
     * Drain a compressed rotated file from uncompressed {@code startOffset},
     * publishing each line. Returns {@code -1} when the file is fully drained
     * (EOF); otherwise the offset to resume from after a publish failure. The
     * cursor is updated as progress is made, so a crash mid-drain resumes
     * correctly. gz streams have no random seek, so a retry re-opens and
     * re-skips — bounded by the 50 MB rotated-file size.
     */
    private long drainGz(File gz, long startOffset, Publisher publisher, int idx) {
        Identity id = identityOf(gz);
        try (InputStream in = new BufferedInputStream(new GZIPInputStream(new FileInputStream(gz)))) {
            long start = skipFully(in, startOffset);
            long[] consumed = { start };
            while (!Thread.currentThread().isInterrupted()) {
                long lineStart = consumed[0];
                String line = readLineCounting(in, consumed);
                if (line == null) return -1; // EOF → fully drained
                if (!handleLine(line, publisher)) {
                    System.out.println("[" + logType + "] Publish FAILED (gz) – will retry in 5s");
                    cursorMgr.update(streamKey, idx, lineStart, id.fileKey(), id.size(), id.headSig());
                    return lineStart;
                }
                cursorMgr.update(streamKey, idx, consumed[0], id.fileKey(), id.size(), id.headSig());
            }
            return consumed[0]; // interrupted
        } catch (IOException e) {
            System.out.println("[" + logType + "] gz read error on " + gz.getName() + ": " + e.getMessage());
            return startOffset; // retry from the start of this attempt
        }
    }

    /**
     * Parse, dedup, and publish one raw line. Returns true if the line was
     * published or intentionally skipped (blank / malformed / all rows
     * suppressed); false only if the publisher rejected it (caller retries).
     */
    private boolean handleLine(String line, Publisher publisher) {
        if (line.isBlank()) return true;
        LogWrapper wrapper = processLine(line);
        if (wrapper == null) return true; // malformed — skip (matches prior behavior)
        if (deduplicator != null && "device_metric_batch".equals(wrapper.type())) {
            String filtered = deduplicator.filterBatch(wrapper.data());
            if (filtered == null) return true; // all rows suppressed
            if (!filtered.equals(wrapper.data())) {
                wrapper = new LogWrapper(wrapper.agentSendingTime(), filtered,
                                         wrapper.type(), wrapper.hostname(), wrapper.ipAddr());
            }
        }
        return publisher.publish(topicPrefix, logType, wrapper);
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
            // Occasional parse failures are expected when tailing an actively-written
            // file — the reader may see a partial line before the writer flushes.
            System.err.println("[LogTailer] Skipping malformed line: " + e.getMessage());
            return null;
        }
    }

    /**
     * Read a line from a RandomAccessFile, decoding bytes as UTF-8.
     * Handles both {@code \n} and {@code \r\n} line endings.
     * Returns null at EOF or on an incomplete (unterminated) line.
     */
    private static String readLineUtf8(RandomAccessFile raf) throws IOException {
        long startPos = raf.getFilePointer();
        var buf = new ByteArrayOutputStream(512);
        int b;
        boolean foundNewline = false;
        while ((b = raf.read()) != -1) {
            if (b == '\n') { foundNewline = true; break; }
            if (b == '\r') {
                foundNewline = true;
                long pos = raf.getFilePointer();
                int next = raf.read();
                if (next != '\n' && next != -1) raf.seek(pos);
                break;
            }
            buf.write(b);
        }
        if (!foundNewline) {
            raf.seek(startPos);
            return null;
        }
        return buf.toString(StandardCharsets.UTF_8);
    }

    /**
     * Read one UTF-8 line from a plain InputStream, advancing {@code consumed[0]}
     * by the exact number of bytes read (including the terminating {@code \n}).
     * NDJSON uses {@code \n} line endings. Returns null at clean EOF.
     */
    private static String readLineCounting(InputStream in, long[] consumed) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream(256);
        int b;
        boolean any = false;
        while ((b = in.read()) != -1) {
            any = true;
            consumed[0]++;
            if (b == '\n') return buf.toString(StandardCharsets.UTF_8);
            buf.write(b);
        }
        if (!any) return null; // clean EOF
        return buf.toString(StandardCharsets.UTF_8); // final line without trailing newline
    }

    /** Skip exactly {@code n} bytes from {@code in}, forcing reads when skip()
     *  returns 0. Returns the number actually skipped (less than n only at EOF). */
    private static long skipFully(InputStream in, long n) throws IOException {
        long remaining = n;
        long total = 0;
        byte[] tmp = new byte[8192];
        while (remaining > 0) {
            long s = in.skip(remaining);
            if (s > 0) {
                total += s;
                remaining -= s;
                continue;
            }
            int toRead = (int) Math.min(tmp.length, remaining);
            int r = in.read(tmp, 0, toRead);
            if (r == -1) break; // EOF before reaching n
            total += r;
            remaining -= r;
        }
        return total;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
