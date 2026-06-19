package com.gpuflight.agent.publisher;

import com.gpuflight.agent.model.LogWrapper;

import java.io.Closeable;
import java.util.List;

public interface Publisher extends Closeable {
    /** Returns true if the event was accepted by the backend, false on failure. */
    boolean publish(String topic, String key, LogWrapper log);

    /** Returns true if the NDJSON batch was accepted by the backend. */
    default boolean publishStream(String sessionId, List<String> ndjsonLines) {
        return false;
    }

    /**
     * Upload a complete, already-gzipped NDJSON window verbatim as one chunk - the
     * client's {@code <channel>.<index>.log.gz} sent byte-for-byte. It is exactly the
     * gzipped-NDJSON wire format {@link #publishStream} produces, so this skips the
     * decompress + re-batch + re-gzip round-trip. Default unsupported - the window
     * fast-path is gated to the HTTP stream publisher.
     */
    default boolean publishStreamGz(String sessionId, byte[] gzBody) {
        return false;
    }

    /**
     * Signal the backend that EVERY channel of {@code sessionId} has finished
     * uploading — the agent has drained all per-channel tailers and every batch
     * was accepted. Lets the backend finalize the session immediately instead of
     * waiting out its conservative grace/settle window.
     *
     * <p>Returns true when the signal was delivered OR is pointless to retry
     * (2xx, or a 4xx like 404 from an older backend without the endpoint); false
     * only on a transient failure (5xx / network) worth retrying. Default no-op
     * (e.g. Kafka, where there is no such finalize step) returns true.
     */
    default boolean publishSessionComplete(String sessionId) {
        return true;
    }
}
