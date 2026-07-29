package com.gpuflight.agent;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import com.gpuflight.agent.config.JsonSettings;
import com.gpuflight.agent.config.StreamUploadSettings;
import com.gpuflight.agent.filter.DeviceMetricDeduplicator;
import com.gpuflight.agent.model.LogWrapper;
import com.gpuflight.agent.model.WindowMetadata;
import com.gpuflight.agent.publisher.Publisher;
import com.gpuflight.agent.publisher.WindowPublishResult;
import com.gpuflight.agent.util.Delays;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.zip.CRC32;
import java.util.zip.GZIPInputStream;

/**
 * Tails the log files written by the C++ gpufl client and ships them to the backend.
 *
 * The client publishes each rotation as a COMPLETE, immutable window file in the
 * session dir: {@code <folder>/<sessionId>/<channel>.<index>.log[.gz]}, where the
 * index increments (1 = oldest, higher = newer). The live active file is written
 * inside {@code <sessionId>/.tmp/} and is never tailed directly, so this tailer
 * simply sends each finished window WHOLE, in increasing index order, and finishes
 * when the session's {@code .tmp/} dir is gone (the client removes it once every
 * channel has closed) and no further window has appeared.
 *
 * Cursor: {@code fileIndex} = the window index in progress, {@code offset} = bytes
 * already sent within it (for mid-window resume after a crash). One LogTailer per
 * (sessionId, channel) pair.
 */
public class LogTailer {
    private final File folder;
    private final String sessionId;
    private final String logType;
    private final String topicPrefix;
    private final CursorManager cursorMgr;
    private final BlockingQueue<Path> consumedFilesQueue; // nullable
    private final DeviceMetricDeduplicator deduplicator;    // nullable
    private final StreamUploadSettings streamUploadSettings;
    private final String streamKey;

    /** Bytes of uncompressed head used for the content signature. */
    private static final int HEAD_SIG_BYTES = 512;
    private static final long PERMANENT_WINDOW_FAILURE = Long.MIN_VALUE;
    private String permanentWindowFailureReason;

    public LogTailer(File folder, String sessionId, String logType, String topicPrefix,
                     CursorManager cursorMgr, BlockingQueue<Path> consumedFilesQueue) {
        this(folder, sessionId, logType, topicPrefix, cursorMgr, consumedFilesQueue, null);
    }

    public LogTailer(File folder, String sessionId, String logType, String topicPrefix,
                     CursorManager cursorMgr, BlockingQueue<Path> consumedFilesQueue,
                     DeviceMetricDeduplicator deduplicator) {
        this(folder, sessionId, logType, topicPrefix, cursorMgr, consumedFilesQueue,
             deduplicator, StreamUploadSettings.DISABLED);
    }

    public LogTailer(File folder, String sessionId, String logType, String topicPrefix,
                     CursorManager cursorMgr, BlockingQueue<Path> consumedFilesQueue,
                     DeviceMetricDeduplicator deduplicator,
                     StreamUploadSettings streamUploadSettings) {
        this.folder = folder;
        this.sessionId = sessionId;
        this.logType = logType;
        this.topicPrefix = topicPrefix;
        this.cursorMgr = cursorMgr;
        this.consumedFilesQueue = consumedFilesQueue;
        this.deduplicator = deduplicator;
        this.streamUploadSettings = streamUploadSettings == null
            ? StreamUploadSettings.DISABLED
            : streamUploadSettings;
        // Cursor key namespaces by session_id so two concurrent sessions'
        // device channels (say `s1.device` and `s2.device`) don't share
        // a cursor position. Survives restart - sessions in
        // cursor.json that are no longer on disk get pruned lazily.
        this.streamKey = sessionId + "." + logType;
    }

    /** The session subdirectory under the watched folder. */
    private File sessionDir() {
        return new File(folder, sessionId);
    }

    /**
     * The rotated file for {@code index} (>=1). Prefers the uncompressed
     * {@code .index.log}; falls back to {@code .index.log.gz} once the client
     * has compressed it. Returns null if neither exists.
     */
    private File resolveRotated(int index) {
        File plain = new File(sessionDir(), logType + "." + index + ".log");
        if (plain.exists()) return plain;
        File gz = new File(sessionDir(), logType + "." + index + ".log.gz");
        return gz.exists() ? gz : null;
    }

    private WindowMetadata metadataFor(File window, int index)
            throws IOException {
        Path metadataPath = metadataPath(index);
        if (!Files.exists(metadataPath)) {
            SessionOwnership.State ownership =
                SessionOwnership.probe(sessionDir().toPath());
            if (ownership == SessionOwnership.State.LEGACY_NO_LOCK) {
                return null; // backward-compatible window from an older client
            }
            // A current client publishes identity before it makes the payload
            // visible. Missing metadata is therefore either the tiny recovery
            // interval of an older lock-aware build or a damaged contract, not
            // permission to silently downgrade exact-once upload to legacy.
            throw new IOException(
                "identity metadata not visible yet for current-client payload "
                    + window.getName());
        }
        WindowMetadata metadata;
        try {
            metadata = JsonSettings.MAPPER.readValue(
                    metadataPath.toFile(), WindowMetadata.class);
        } catch (JacksonException malformed) {
            throw new PermanentWindowException(
                    "window identity metadata is malformed for "
                            + window.getName());
        }
        if (!metadata.isValidFor(
                sessionId, logType, index, window.getName())) {
            throw new PermanentWindowException(
                "window metadata does not match payload " + window.getName());
        }
        return metadata;
    }

    private Path metadataPath(int index) {
        return sessionDir().toPath().resolve(
                ".gpufl-window." + logType + "." + index + ".json");
    }

    /**
     * A crash after persisting the post-ACK cursor but before local cleanup
     * must not leak payloads forever. On restart, re-enqueue only windows
     * proven identity-aware by their tombstones; legacy cursor history keeps
     * its prior retention behavior.
     */
    private void enqueuePreviouslyAcknowledgedWindows(int nextIndex) {
        if (consumedFilesQueue == null || !streamUploadSettings.enabled()) return;
        for (int index = 1; index < nextIndex; ++index) {
            File payload = resolveRotated(index);
            if (payload != null && isGz(payload)
                    && Files.isRegularFile(metadataPath(index))
                    && AcknowledgedWindowCleaner.hasBackendAcknowledgement(
                        sessionDir().toPath(), logType, index)) {
                consumedFilesQueue.offer(payload.toPath());
            }
        }
    }

    private static boolean checksumMatches(
            WindowMetadata metadata, byte[] payload) {
        if (metadata == null) return true;
        CRC32 crc = new CRC32();
        crc.update(payload);
        return metadata.payloadBytes() == payload.length
                && metadata.payloadCrc32() == crc.getValue();
    }

    private static boolean isGz(File f) {
        return f.getName().endsWith(".gz");
    }

    private static final class StreamBatch {
        private final int maxLines;
        private final long maxBytes;
        private final List<String> lines = new ArrayList<>();
        private long bytes;
        private long startOffset = -1L;
        private long endOffset;

        StreamBatch(StreamUploadSettings settings) {
            this.maxLines = Math.max(1, settings.maxLines());
            this.maxBytes = Math.max(1L, settings.maxBytes());
        }

        boolean isEmpty() {
            return lines.isEmpty();
        }

        long startOffset() {
            return startOffset;
        }

        long endOffset() {
            return endOffset;
        }

        int lineCount() {
            return lines.size();
        }

        long bytes() {
            return bytes;
        }

        void add(String line, long lineStart, long lineEnd) {
            if (lines.isEmpty()) {
                startOffset = lineStart;
            }
            lines.add(line);
            bytes += line.getBytes(StandardCharsets.UTF_8).length + 1L;
            endOffset = lineEnd;
        }

        boolean shouldFlush() {
            return lines.size() >= maxLines || bytes >= maxBytes;
        }

        List<String> lines() {
            return List.copyOf(lines);
        }

        void clear() {
            lines.clear();
            bytes = 0L;
            startOffset = -1L;
            endOffset = 0L;
        }
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
     * compression - it is the signal that lets a restart find the file it was
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

    /**
     * Blocking tail loop (virtual thread). The client publishes each rotation as a
     * COMPLETE, immutable window file {@code <channel>.<index>.log[.gz]} (index 1 =
     * oldest, monotonic incrementing). We send each finished window WHOLE, in
     * increasing index order, and finish when the session's {@code .tmp/} working
     * dir is gone - the client removes it once every channel has closed - and no
     * further window has appeared. No partial-file tailing, no rotation-shift guess.
     */
    public boolean tail(Publisher publisher) {
        var cursor = cursorMgr.get(streamKey);
        // fileIndex = window index in progress (1-based; 0 = none yet); offset =
        // bytes already sent within it (mid-window resume after a crash).
        int idx = Math.max(1, cursor.fileIndex());
        long offset = cursor.fileIndex() >= 1 ? cursor.offset() : 0L;
        if (SessionOwnership.hasAgentWindowFailure(
                sessionDir().toPath(), logType, idx)) {
            System.err.println("[" + logType + "] prior permanent transport "
                    + "window failure is still present; refusing "
                    + "session-complete");
            return true;
        }
        enqueuePreviouslyAcknowledgedWindows(idx);
        System.out.println("[" + logType + "] Starting (window mode) - window index=" + idx + ", offset=" + offset);

        while (!Thread.currentThread().isInterrupted()) {
            File window = resolveRotated(idx);
            if (window != null) {
                long resume = drainWindow(window, offset, publisher, idx);
                if (resume == PERMANENT_WINDOW_FAILURE) {
                    String reason = permanentWindowFailureReason == null
                            ? "immutable_window_contract_violation"
                            : permanentWindowFailureReason;
                    boolean marked =
                            SessionOwnership.recordAgentWindowFailure(
                                    sessionDir().toPath(), logType, idx, reason);
                    System.err.println("[" + logType + "] permanent transport "
                            + "window failure at " + window.getName() + ": "
                            + reason + ". Payload retained; session-complete "
                            + "will not be emitted."
                            + (marked ? ""
                                : " WARNING: durable loss marker write failed."));
                    if (marked) return true;
                    // Never fall through to session-complete without a
                    // durable indication that this profile is incomplete.
                    if (!Delays.sleep(Delays.LOG_TAILER_RETRY)) break;
                } else if (resume < 0) {
                    System.out.println("[" + logType + "] Sent window " + window.getName() + ".");
                    if (consumedFilesQueue != null) consumedFilesQueue.offer(window.toPath());
                    idx++;
                    offset = 0L;
                    cursorMgr.update(streamKey, idx, offset);
                } else {
                    // Publish failed mid-window; the cursor already holds the resume offset.
                    offset = resume;
                    if (!Delays.sleep(Delays.LOG_TAILER_RETRY)) break;
                }
                continue;
            }

            // The backend ACK can be durable before local payload cleanup and
            // cursor persistence. If cleanup won that race and this agent then
            // crashed, the ACK tombstone is enough to skip the absent payload
            // and continue with the next sequence on restart.
            if (AcknowledgedWindowCleaner.hasBackendAcknowledgement(
                    sessionDir().toPath(), logType, idx)) {
                idx++;
                offset = 0L;
                cursorMgr.update(streamKey, idx, offset);
                continue;
            }

            // Window <idx> not published yet. Is the session still writing?
            File tmp = sessionTmpDir();
            if (tmp.exists()) {
                SessionOwnership.State ownership =
                    SessionOwnership.probe(sessionDir().toPath());
                if (ownership == SessionOwnership.State.ACTIVE
                        || ownership == SessionOwnership.State.UNKNOWN) {
                    // A quiet workload can leave `.tmp` unchanged for much
                    // longer than the legacy mtime grace. The OS lock is the
                    // authoritative liveness signal and survives that silence.
                    if (!Delays.sleep(Delays.LOG_TAILER_POLL)) break;
                    continue;
                }
                if (ownership == SessionOwnership.State.UNOWNED) {
                    System.out.println("[" + logType + "] Session finished "
                            + "(ownership lock released - client gone; last sent "
                            + (idx - 1) + ").");
                    return true;
                }
                // A live client keeps appending to .tmp/ (system sampling alone
                // advances its mtime); a client that crashed or was killed leaves
                // .tmp/ frozen and never removed. Once it has sat unchanged past the
                // grace, treat it as orphaned and finish - one dead session must not
                // strand the agent's drain (and the 60s --upload cap) forever.
                if (System.currentTimeMillis() - newestMtime(tmp) > Delays.STALE_TMP_GRACE.toMillis()) {
                    System.out.println("[" + logType + "] Session finished (.tmp stale > "
                            + Delays.STALE_TMP_GRACE.toSeconds() + "s - client gone; last sent " + (idx - 1) + ").");
                    return true; // orphaned: producer never removed .tmp/
                }
                if (!Delays.sleep(Delays.LOG_TAILER_POLL)) break;
                continue;
            }

            // .tmp/ gone -> the client closed every channel. Each channel's last
            // window is published BEFORE .tmp/ is removed, so wait a moment then do
            // one final check for a straggler before finishing.
            SessionOwnership.State ownership =
                SessionOwnership.probe(sessionDir().toPath());
            if (ownership == SessionOwnership.State.ACTIVE
                    || ownership == SessionOwnership.State.UNKNOWN) {
                if (!Delays.sleep(Delays.LOG_TAILER_POLL)) break;
                continue;
            }
            if (!Delays.sleep(Delays.SESSION_END_GRACE_PERIOD)) break;
            if (resolveRotated(idx) != null
                    || SessionOwnership.probe(sessionDir().toPath())
                        == SessionOwnership.State.ACTIVE) {
                continue;
            }
            System.out.println("[" + logType + "] Session finished (.tmp gone, no window "
                    + idx + "; last sent " + (idx - 1) + ").");
            return false; // clean: producer closed and removed .tmp/
        }
        return false; // interrupted before finishing
    }

    /** The client's per-session working dir {@code <folder>/<sessionId>/.tmp}: active
     *  files live here while writing; the client removes it once every channel has
     *  closed, which is our "no more windows" signal. */
    private File sessionTmpDir() {
        return new File(sessionDir(), ".tmp");
    }

    /** Newest last-modified time of {@code dir} and its immediate entries. The
     *  client appends to {@code .tmp/<channel>.log} while writing, so this advances
     *  as long as the session is live and freezes once the client is gone.
     *  Package-private so the drain-exit gate (GpuflAgent) shares the tailer's
     *  staleness notion. */
    static long newestMtime(File dir) {
        long newest = dir.lastModified();
        File[] kids = dir.listFiles();
        if (kids != null) {
            for (File k : kids) {
                long m = k.lastModified();
                if (m > newest) newest = m;
            }
        }
        return newest;
    }

    /**
     * Drain a compressed rotated file from uncompressed {@code startOffset},
     * publishing each line. Returns {@code -1} when the file is fully drained
     * (EOF); otherwise the offset to resume from after a publish failure. The
     * cursor is updated as progress is made, so a crash mid-drain resumes
     * correctly. gz streams have no random seek, so a retry re-opens and
     * re-skips - bounded by the 50 MB rotated-file size.
     */
    private long drainWindow(File window, long startOffset, Publisher publisher, int idx) {
        // A published window is a COMPLETE gzipped NDJSON file - exactly the stream
        // wire format - so in stream mode send it VERBATIM as one chunk (no decompress
        // / re-batch / re-gzip). Returns -1 (sent) or 0 (retry from the start). Plain
        // (.log) windows from a no-compressor build fall through to the line path below.
        if (streamUploadSettings.enabled() && isGz(window)) {
            try {
                WindowMetadata metadata = metadataFor(window, idx);
                byte[] gz = Files.readAllBytes(window.toPath());
                if (!checksumMatches(metadata, gz)) {
                    permanentWindowFailureReason =
                            "payload_checksum_mismatch:" + window.getName();
                    return PERMANENT_WINDOW_FAILURE;
                }
                WindowPublishResult published = metadata == null
                        ? (publisher.publishStreamGz(sessionId, gz)
                            ? WindowPublishResult.acceptedLegacy()
                            : WindowPublishResult.retry())
                        : publisher.publishTransportWindow(
                                sessionId, metadata, gz);
                if (published.accepted()) {
                    if (published.identityAcknowledged()
                            && !AcknowledgedWindowCleaner
                                .recordBackendAcknowledgement(
                                    sessionDir().toPath(), metadata)) {
                        // The backend can safely deduplicate the retry. Do not
                        // advance until deletion authorization is durable.
                        return 0L;
                    }
                    // Persist the window-done advance right after the durable 2xx so a crash
                    // before tail() advances won't re-send the whole window on restart (the
                    // line path likewise persists the cursor after each accepted batch).
                    cursorMgr.update(streamKey, idx + 1, 0L);
                    return -1L;
                }
                System.out.println("[" + logType + "] window publish FAILED - retry in 5s");
                return 0L;
            } catch (PermanentWindowException e) {
                permanentWindowFailureReason = e.getMessage();
                return PERMANENT_WINDOW_FAILURE;
            } catch (IOException e) {
                System.out.println("[" + logType + "] read error on " + window.getName() + ": " + e.getMessage());
                return 0L;
            }
        }
        Identity id = identityOf(window);
        try (InputStream in = isGz(window)
                ? new BufferedInputStream(new GZIPInputStream(new FileInputStream(window)))
                : new BufferedInputStream(new FileInputStream(window))) {
            long start = skipFully(in, startOffset);
            long[] consumed = { start };
            if (streamUploadSettings.enabled()) {
                StreamBatch batch = new StreamBatch(streamUploadSettings);
                while (!Thread.currentThread().isInterrupted()) {
                    long lineStart = consumed[0];
                    String line = readLineCounting(in, consumed);
                    if (line == null) {
                        if (!batch.isEmpty()) {
                            if (!flushStreamBatch(batch, publisher)) {
                                System.out.println("[" + logType + "] Stream publish FAILED (gz) - will retry in 5s");
                                cursorMgr.update(streamKey, idx, batch.startOffset(),
                                                 id.fileKey(), id.size(), id.headSig());
                                return batch.startOffset();
                            }
                            cursorMgr.update(streamKey, idx, batch.endOffset(),
                                             id.fileKey(), id.size(), id.headSig());
                        }
                        return -1; // EOF -> fully drained
                    }

                    String prepared = prepareStreamLine(line);
                    if (prepared == null) {
                        cursorMgr.update(streamKey, idx, consumed[0], id.fileKey(), id.size(), id.headSig());
                        continue;
                    }
                    batch.add(prepared, lineStart, consumed[0]);
                    if (batch.shouldFlush()) {
                        if (!flushStreamBatch(batch, publisher)) {
                            System.out.println("[" + logType + "] Stream publish FAILED (gz) - will retry in 5s");
                            cursorMgr.update(streamKey, idx, batch.startOffset(),
                                             id.fileKey(), id.size(), id.headSig());
                            return batch.startOffset();
                        }
                        cursorMgr.update(streamKey, idx, batch.endOffset(),
                                         id.fileKey(), id.size(), id.headSig());
                        batch.clear();
                    }
                }
                return batch.isEmpty() ? consumed[0] : batch.startOffset();
            }
            while (!Thread.currentThread().isInterrupted()) {
                long lineStart = consumed[0];
                String line = readLineCounting(in, consumed);
                if (line == null) return -1; // EOF -> fully drained
                if (!handleLine(line, publisher)) {
                    System.out.println("[" + logType + "] Publish FAILED (gz) - will retry in 5s");
                    cursorMgr.update(streamKey, idx, lineStart, id.fileKey(), id.size(), id.headSig());
                    return lineStart;
                }
                cursorMgr.update(streamKey, idx, consumed[0], id.fileKey(), id.size(), id.headSig());
            }
            return consumed[0]; // interrupted
        } catch (IOException e) {
            System.out.println("[" + logType + "] window read error on " + window.getName() + ": " + e.getMessage());
            return startOffset; // retry from the start of this attempt
        }
    }

    private boolean flushStreamBatch(StreamBatch batch, Publisher publisher) {
        if (batch.isEmpty()) {
            return true;
        }
        System.out.println("[" + logType + "] Sending stream batch: session=" + sessionId
            + " lines=" + batch.lineCount()
            + " bytes=" + batch.bytes()
            + " offset=" + batch.startOffset() + "->" + batch.endOffset());
        boolean accepted = publisher.publishStream(sessionId, batch.lines());
        if (accepted) {
            System.out.println("[" + logType + "] Stream batch accepted: session=" + sessionId
                + " lines=" + batch.lineCount()
                + " nextOffset=" + batch.endOffset());
        } else {
            System.out.println("[" + logType + "] Stream batch rejected: session=" + sessionId
                + " retryOffset=" + batch.startOffset());
        }
        return accepted;
    }

    /**
     * Validate and optionally deduplicate one raw NDJSON line for stream upload.
     * Returns null when the line should be skipped.
     */
    private String prepareStreamLine(String line) {
        if (line.isBlank()) return null;
        try {
            JsonNode node = JsonSettings.MAPPER.readTree(line);
            String type = node.path("type").asText("unknown");
            if (deduplicator != null && "device_metric_batch".equals(type)) {
                return deduplicator.filterBatch(line);
            }
            return line;
        } catch (Exception e) {
            // Occasional parse failures are expected when tailing an actively-written
            // file - the reader may see a partial line before the writer flushes.
            System.err.println("[LogTailer] Skipping malformed line: " + e.getMessage());
            return null;
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
        if (wrapper == null) return true; // malformed - skip (matches prior behavior)
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
            // file - the reader may see a partial line before the writer flushes.
            System.err.println("[LogTailer] Skipping malformed line: " + e.getMessage());
            return null;
        }
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

    private static final class PermanentWindowException extends IOException {
        PermanentWindowException(String message) {
            super(message);
        }
    }
}
