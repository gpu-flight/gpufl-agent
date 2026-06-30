package com.gpuflight.agent.util;

import java.time.Duration;

/**
 * Centralized operational delays and sleep utilities.
 * Using named constants improves readability and makes the agent's
 * timing behavior easier to tune.
 */
public final class Delays {

    private Delays() {}

    /** Polling interval for discovering new sessions in watched folders. */
    public static final Duration SESSION_WATCHER_POLL = Duration.ofSeconds(2);

    /** Polling interval for checking if all sessions have drained before exiting. */
    public static final Duration DRAIN_CHECK_POLL = Duration.ofSeconds(1);

    /** Initial delay between retries when signaling session completion to the backend. */
    public static final Duration SESSION_COMPLETE_RETRY = Duration.ofSeconds(2);

    /** Delay before retrying a failed window upload. */
    public static final Duration LOG_TAILER_RETRY = Duration.ofSeconds(5);

    /** Polling interval when waiting for a new window to be published in an active session. */
    public static final Duration LOG_TAILER_POLL = Duration.ofSeconds(2);

    /**
     * Grace period after a session's .tmp directory is gone before the final check for
     * straggler windows. This ensures any late flushes are noticed.
     */
    public static final Duration SESSION_END_GRACE_PERIOD = Duration.ofMillis(4500);

    /**
     * How long a session's {@code .tmp/} working dir may sit unchanged - no new
     * window, no write - before the tailer treats it as ORPHANED (left behind by a
     * client that crashed or was killed and never removed it) and finishes anyway.
     * A live client keeps writing {@code .tmp/} - system sampling alone advances it
     * well inside this window - so this only fires on genuinely dead sessions, and
     * stops one of them from stranding the agent's drain forever.
     */
    public static final Duration STALE_TMP_GRACE = Duration.ofSeconds(30);

    /**
     * Sleep for the specified duration.
     * @return true if the sleep finished normally, false if it was interrupted.
     */
    public static boolean sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
