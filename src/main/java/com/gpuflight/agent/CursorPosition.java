package com.gpuflight.agent;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A persisted tail position for one log stream.
 *
 * <p>{@code fileIndex}/{@code offset} are the original cursor. The identity
 * fields ({@code fileKey}, {@code fileSize}, {@code createdMillis}) were added
 * to make restarts robust against rotation: on startup the tailer compares the
 * saved identity against the current active file, so it can tell when the file
 * it was reading has been rotated away (and resume from the rotated/gz copy)
 * instead of silently resetting to offset 0.
 *
 * <p>Two identity signals, because the client both renames AND compresses on
 * rotation:
 *   - {@code fileKey} (inode on POSIX) survives a rename/shift (active →
 *     {@code .1.log} → {@code .2.log}) but NOT compression — gzip writes a new
 *     file ({@code .1.log.gz}) with a different inode. Fast path on Linux while
 *     the rotated file is still uncompressed.
 *   - {@code headSig} (CRC32 of the first bytes of the *uncompressed* content)
 *     survives BOTH rename and compression, since the content is identical.
 *     This is the robust matcher used to find "my file" even after it became a
 *     {@code .gz}.
 *
 * <p>Identity fields are nullable/zero and default that way, so cursor files
 * written before this change still deserialize cleanly (identity unknown →
 * legacy behavior).
 */
public record CursorPosition(
    @JsonProperty("fileIndex") int    fileIndex,
    @JsonProperty("offset")    long   offset,
    @JsonProperty("fileKey")   String fileKey,   // inode/file-id; survives rename, not compression. null on Windows.
    @JsonProperty("fileSize")  long   fileSize,   // size at last update (sanity check)
    @JsonProperty("headSig")   long   headSig     // CRC32 of first uncompressed bytes; survives compression. 0 = unset.
) {
    /** Backward-compatible constructor — identity unknown. */
    public CursorPosition(int fileIndex, long offset) {
        this(fileIndex, offset, null, 0L, 0L);
    }
}
