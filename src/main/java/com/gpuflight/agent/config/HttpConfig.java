package com.gpuflight.agent.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for the HTTP publisher.
 *
 * <p>The destination is split across {@code hostUrl} + {@code apiVersion} —
 * the full path is built internally via {@link #endpointFor(String)} so
 * the user only specifies what changes per-deployment (the host) and
 * occasionally per-rollout (the version). The {@code /api/{version}/events/}
 * path is a structural contract baked into this record.
 *
 * <p>Examples:
 * <pre>
 *   hostUrl="https://api.gpuflight.com"     apiVersion="v1"
 *     → endpointFor("init") = "https://api.gpuflight.com/api/v1/events/init"
 *
 *   hostUrl="http://localhost:8080"         apiVersion="v2"
 *     → endpointFor("init") = "http://localhost:8080/api/v2/events/init"
 * </pre>
 *
 * <p><strong>Migration note (May 2026):</strong> previous releases used a
 * single {@code endpointUrl} field that bundled host + version + path
 * (e.g. {@code "https://api.gpuflight.com/api/v1/events/"}). That shape
 * was removed because it forced every user to re-edit their config on
 * any backend version bump, and the trailing-slash semantics were
 * load-bearing for the upload code. The compact constructor below
 * surfaces a clear error if a legacy config still tries to set null
 * {@code hostUrl}.
 */
public record HttpConfig(
    @JsonProperty("hostUrl")        String hostUrl,
    @JsonProperty("apiVersion")     String apiVersion,
    @JsonProperty("authToken")      String authToken,
    @JsonProperty("timeoutSeconds") long   timeoutSeconds
) implements PublisherConfig {

    /** Default API version when the user doesn't specify one. */
    public static final String DEFAULT_API_VERSION = "v1";

    public HttpConfig {
        // Field-level defaults / normalization. Run inside the compact
        // constructor so they apply to BOTH the JSON-deserialized path
        // (via Jackson) and any direct callers (tests, CLI plumbing).

        if (timeoutSeconds <= 0) {
            timeoutSeconds = 10L;
        }
        if (apiVersion == null || apiVersion.isBlank()) {
            apiVersion = DEFAULT_API_VERSION;
        }
        // Hard reject blank/missing host with a migration-aware error.
        // Jackson silently sets unknown fields to null (we disabled
        // FAIL_ON_UNKNOWN_PROPERTIES in JsonSettings), so a config that
        // still says {"endpointUrl": "..."} would otherwise crash with
        // a confusing NPE at publish time. Catch it here at startup.
        if (hostUrl == null || hostUrl.isBlank()) {
            throw new IllegalArgumentException(
                "HttpConfig.hostUrl is required. Specify just the scheme+host"
              + " (e.g. \"https://api.gpuflight.com\"); the /api/{version}/events/"
              + " path is now built automatically.\n"
              + "  Migration: if your config still uses the legacy `endpointUrl`"
              + " field (e.g. \"https://api.gpuflight.com/api/v1/events/\"), rename"
              + " it to `hostUrl` and drop the path suffix. The api version moved"
              + " to a separate `apiVersion` field (defaults to \"" + DEFAULT_API_VERSION + "\").");
        }
        // Strip trailing slash so endpointFor() can join cleanly with
        // "/api/...". User is allowed to supply either shape; we
        // canonicalize here so the join is deterministic.
        if (hostUrl.endsWith("/")) {
            hostUrl = hostUrl.substring(0, hostUrl.length() - 1);
        }
    }

    /**
     * Build the full upload URL for a given event type.
     *
     * @param type the event-type segment, e.g. {@code "init"},
     *             {@code "kernel"}, {@code "metric"} — appended after
     *             the {@code /api/{version}/events/} prefix.
     */
    public String endpointFor(String type) {
        return hostUrl + "/api/" + apiVersion + "/events/" + type;
    }
}
