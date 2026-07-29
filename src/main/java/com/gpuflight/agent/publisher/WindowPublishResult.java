package com.gpuflight.agent.publisher;

/**
 * Separates backend acceptance from authorization to delete the local payload.
 *
 * <p>An older backend can accept an identity-aware request through its legacy
 * path. That is enough to advance the upload cursor, but not enough to delete
 * the only replayable copy because no transport-window registry row exists.
 */
public record WindowPublishResult(
        boolean accepted,
        boolean identityAcknowledged) {

    public static WindowPublishResult retry() {
        return new WindowPublishResult(false, false);
    }

    public static WindowPublishResult acceptedLegacy() {
        return new WindowPublishResult(true, false);
    }

    public static WindowPublishResult acceptedIdentity() {
        return new WindowPublishResult(true, true);
    }
}
