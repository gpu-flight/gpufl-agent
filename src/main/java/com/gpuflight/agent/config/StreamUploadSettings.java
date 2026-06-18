package com.gpuflight.agent.config;

/**
 * Per-tailer batching knobs for stream upload mode. When {@code enabled}, the
 * tailer collects lines into a batch and hands the whole batch to
 * {@code Publisher.publishStream} - flushing when {@code maxLines} or
 * {@code maxBytes} (uncompressed) is reached, or at EOF (no artificial
 * latency: the moment the file has no more data, whatever is buffered ships).
 * The cursor advances only after a flush is accepted, so a crash or rejected
 * batch re-reads from the last accepted offset.
 *
 * <p>{@code DISABLED} keeps the original one-publish-per-line flow (legacy
 * mode, Kafka, and existing tests).
 */
public record StreamUploadSettings(boolean enabled, int maxLines, long maxBytes) {

    public static final StreamUploadSettings DISABLED = new StreamUploadSettings(false, 0, 0);

    public static StreamUploadSettings from(HttpConfig http) {
        return http.isStreamMode()
            ? new StreamUploadSettings(true, http.streamMaxLines(), http.streamMaxBytes())
            : DISABLED;
    }
}
